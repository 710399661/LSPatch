package org.lsposed.lspatch.service;

import android.annotation.SuppressLint;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Binder;
import android.os.Build;
import android.os.Environment;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.ParcelFileDescriptor;
import android.os.RemoteException;
import android.os.SystemClock;
import android.os.UserHandle;
import android.util.Log;
import android.widget.Toast;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipFile;
import org.lsposed.lspatch.share.Constants;
import org.lsposed.lspatch.util.LoadedModules;
import org.matrix.vector.ipc.IFrameworkService;
import org.matrix.vector.ipc.IProcessChannel;
import org.matrix.vector.ipc.LoadedModule;

/**
 * The {@link IFrameworkService} for manager mode: it binds the manager's service and forwards module
 * queries to it, so the app is served whatever modules the manager has scoped to it.
 *
 * <p>The manager is an ordinary app and can be gone at any moment -- reaped for memory, force-stopped
 * by a person or by whatever the device calls its battery saver. Three things follow, and this class
 * is where all three are handled. The binding is kept and re-established rather than made once, so a
 * manager that dies mid-session comes back on its own. Every module's service is handed to the
 * framework through a {@link ReconnectingModuleService}, so the module never holds a binder into a
 * process that has gone. And when the manager does not answer at startup at all, the modules are
 * loaded from {@link ModuleSnapshot} -- the same APKs, listed from the host's own copy -- rather than
 * the app starting silently unhooked.</p>
 */
public class RemoteApplicationService implements IFrameworkService {

    private static final String TAG = "LSPatch";

    /**
     * How long the app waits for the manager's binder before starting without it.
     *
     * The bind carries BIND_AUTO_CREATE, so this covers starting the manager's process from nothing.
     * The app's own startup is held open meanwhile, which is why it is short: a miss is no longer
     * fatal -- the snapshot answers instead and the binding stays live for whenever the manager does
     * come up -- so there is nothing to buy by waiting longer.
     */
    private static final long BIND_TIMEOUT_MS = 3000;

    private static final long REBIND_DELAY_MS = 2000;
    private static final long REBIND_MAX_DELAY_MS = 300_000;

    private final Context context;
    private final String managerPackage;
    private final ModuleSnapshot snapshot;
    private final ModuleDeliveryLog deliveryLog;
    private final File stateDir;

    private final ScheduledExecutorService worker =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "lspatch-manager-link"));

    /** One per module, for the life of the process; what the framework is handed. */
    private final Map<String, ReconnectingModuleService> moduleServices = new ConcurrentHashMap<>();

    /**
     * This service's own identity, which is the host's and not the manager's.
     *
     * The framework keeps a service only if it can take a binder from it and watch that binder die
     * ({@code VectorServiceClient.init}), so answering with the manager's binder would mean answering
     * with null whenever the manager is away -- and being dropped for the life of the process at the
     * exact moment this class exists to cover. It is a local binder: it never dies, the framework's
     * death watch is therefore a no-op, and the manager's comings and goings are handled here instead
     * of ending the framework's client.
     */
    private final Binder token = new Binder();

    /** Only ever touched through {@link #legacyHandler()}; null on Q and later, where it is never needed. */
    private Handler legacyHandler;

    private volatile IFrameworkService service;
    private volatile boolean bound;
    private volatile boolean everConnected;
    private volatile long rebindDelay = REBIND_DELAY_MS;

    /**
     * The channel the manager drives hot reload over. Created when the framework attaches its own and
     * kept, because the manager loses its side with its process and has to be handed one again.
     */
    private volatile LSPatchProcessChannel processChannel;

    private final ServiceConnection connection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder binder) {
            var manager = IFrameworkService.Stub.asInterface(binder);
            service = manager;
            rebindDelay = REBIND_DELAY_MS;
            deliveryLog.recordDelivered();
            if (!everConnected) {
                everConnected = true;
                Log.i(TAG, "Manager binder received");
                connected.countDown();
                return;
            }
            // A reconnection: the framework asked for its modules long ago and will not ask again, so
            // everything the previous manager process was holding has to be re-established from here.
            Log.i(TAG, "Manager is back; restoring what it was holding");
            worker.execute(() -> reestablish(manager));
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            // The binding survives: with BIND_AUTO_CREATE the system restarts the manager and calls
            // back here on its own, and until it does the module services serve what they cached.
            Log.w(TAG, "Manager service died");
            service = null;
        }

        @Override
        public void onBindingDied(ComponentName name) {
            // Permanent, unlike a death: the package was replaced or force-stopped, and nothing is
            // coming back on this binding. Only an explicit rebind reaches the manager again.
            Log.w(TAG, "Binding to the manager died; will rebind");
            service = null;
            unbind();
            scheduleRebind();
        }

        @Override
        public void onNullBinding(ComponentName name) {
            Log.e(TAG, "Manager refused to serve this app");
            service = null;
            unbind();
            scheduleRebind();
        }
    };

    private final CountDownLatch connected = new CountDownLatch(1);

    /**
     * The manager packages to try, in order: the name this app was patched against (recorded in the
     * patch config), then the stock package id. Matches the order used by the meta-loader so that a
     * cloak that the loader bootstrap tolerates is also tolerated when *binding* the manager's
     * service. Otherwise a cloak-rebrand or a cloak-revert leaves the loader trying to reach a
     * package that is no longer installed: bind() then returns false straight away and, with no
     * saved module snapshot, the app is reported "running unhooked" with the opaque "LSPatch
     * manager not reachable" toast.
     */
    private static List<String> managerCandidates(String recorded) {
        var ordered = new LinkedHashSet<String>(2);
        if (recorded != null && !recorded.isEmpty()) ordered.add(recorded);
        ordered.add(Constants.MANAGER_PACKAGE_NAME);
        return new ArrayList<>(ordered);
    }

    /** Whether the candidate APK actually carries the loader dex asset -- same shape check the meta-
     *  loader uses, so we don't settle for a package whose name collides but has no loader in it. */
    private static boolean carriesLoader(String sourceDir) {
        if (sourceDir == null) return false;
        try (var zip = new ZipFile(new File(sourceDir))) {
            return zip.getEntry(Constants.LOADER_DEX_ASSET_PATH) != null;
        } catch (Throwable t) {
            return false;
        }
    }

    private String resolveManagerPackage(List<String> candidates) {
        var pm = context.getPackageManager();
        String fallback = null;
        for (String candidate : candidates) {
            try {
                var info = pm.getApplicationInfo(candidate, 0);
                if (info == null || info.sourceDir == null) continue;
                // First candidate that is actually installed wins. The meta-loader has already
                // confirmed one of these candidates carries the loader (we would not be starting
                // in manager mode otherwise), so a simple PM-level check is enough here to skip
                // uninstalled recorded names after a cloak revert.
                if (fallback == null) fallback = candidate;
                if (carriesLoader(info.sourceDir)) {
                    Log.i(TAG, "Resolved manager candidate " + candidate + " (sourceDir carries loader)");
                    return candidate;
                }
            } catch (Throwable t) {
                // Package not visible to us (queries filter / not installed for this user). The
                // meta-loader has a privileged view (IPackageManager + HiddenApiBypass) and may
                // have succeeded even when the app's PackageManager cannot see the package, so
                // we swallow this and keep the candidate for the plain bindService() attempt.
                Log.w(TAG, "Manager candidate " + candidate + " is not visible to PackageManager", t);
            }
        }
        if (fallback != null) {
            Log.w(TAG, "No manager candidate passed the loader check; falling back to installed candidate " + fallback);
            return fallback;
        }
        // None of the candidates were visible through PackageManager. Use the recorded / stock
        // order and let bindService() surface the failure; preserving the original behavior.
        return candidates.get(0);
    }

    public RemoteApplicationService(Context context, String managerPackageName) {
        this.context = context;
        var candidates = managerCandidates(
                (managerPackageName == null || managerPackageName.isEmpty())
                        ? null : managerPackageName);
        this.managerPackage = resolveManagerPackage(candidates);
        this.stateDir = new File(context.getNoBackupFilesDir(), "lspatch");
        this.snapshot = new ModuleSnapshot(context);
        this.deliveryLog = new ModuleDeliveryLog(context);

        // Attempt each visible candidate in order. The first bind() that the system accepts is
        // kept; if it times out unanswered, the binding is left in place (so a late answer still
        // propagates through onServiceConnected), matching the historical single-candidate
        // behavior. Earlier candidates are unbound cleanly before moving on, so the system does
        // not accumulate orphan binding records when the recorded name is stale.
        var start = SystemClock.elapsedRealtime();
        long timeBudgetMs = BIND_TIMEOUT_MS;
        List<String> tried = new ArrayList<>(candidates.size());
        boolean anyBindAccepted = false;

        // Build the ordered search list: prefer the resolved manager, then fall back to the
        // remaining candidates in case resolveManagerPackage chose wrongly (e.g. because the
        // PM query was filtered and picked an installed-but-empty package, while another
        // candidate would actually bind).
        var search = new LinkedHashSet<String>();
        search.add(this.managerPackage);
        search.addAll(candidates);

        for (String candidate : search) {
            tried.add(candidate);
            // Swap the currently-targeted package for the duration of this candidate attempt.
            String prev = this.managerPackage;
            this.managerPackage = candidate;
            Log.i(TAG, "Request manager binder from " + candidate);
            long stepStart = SystemClock.elapsedRealtime();
            boolean boundNow = bind();
            if (!boundNow) {
                this.managerPackage = prev;
                Log.e(TAG, "System refused to bind " + candidate + "; trying next candidate");
                continue;
            }
            anyBindAccepted = true;
            long remaining = Math.max(100L, timeBudgetMs - (SystemClock.elapsedRealtime() - start));
            try {
                if (connected.await(remaining, TimeUnit.MILLISECONDS)) {
                    Log.i(TAG, "Manager binder received in "
                            + (SystemClock.elapsedRealtime() - start) + "ms"
                            + " (tried " + tried.size() + " candidate(s))");
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            long elapsedStep = SystemClock.elapsedRealtime() - stepStart;
            if (elapsedStep >= remaining - 50L) {
                // Budget exhausted on this candidate. Leave the binding in place for a late
                // answer; do not try further candidates because the app's own startup is being
                // held open waiting on this constructor to return.
                Log.w(TAG, "Manager " + candidate + " did not answer in " + elapsedStep
                        + "ms; budget exhausted, not trying remaining candidates");
                break;
            }
            // Candidate accepted the bind but onServiceConnected never fired within the step
            // slice: unbind it cleanly before moving to the next candidate so we do not leave
            // the system holding a dead binding.
            unbind();
            this.managerPackage = prev;
            Log.w(TAG, "Manager " + candidate + " did not answer in " + elapsedStep
                    + "ms; moving to next candidate");
        }

        // If we get here, no candidate delivered a binder in time.
        long totalMs = SystemClock.elapsedRealtime() - start;
        if (anyBindAccepted) {
            Log.w(TAG, "Manager did not answer in " + totalMs + "ms (tried: " + tried + ")");
        } else {
            Log.e(TAG, "System refused to bind any manager candidate (" + tried
                    + "); manager may not be installed");
            scheduleRebind();
        }
        deliveryLog.recordFallback();
        if (snapshot.isEmpty()) {
            // Nothing cached and nobody to ask: this app is genuinely running unhooked, and that
            // is worth telling the person holding the phone -- nothing else will. Be specific
            // about which package(s) were attempted so diagnosing a cloak mismatch is easier.
            toast("LSPatch manager not reachable (tried: " + String.join(", ", tried) + ")");
        } else {
            Log.i(TAG, "Loading modules from this app's own snapshot (" + totalMs + "ms startup timeout)");
        }
    }

    @SuppressLint("DiscouragedPrivateApi")
    private boolean bind() {
        var intent = new Intent()
                .setComponent(new ComponentName(managerPackage, Constants.MANAGER_SERVICE_NAME))
                .putExtra("packageName", context.getPackageName())
                // The manager may have been force-stopped by a battery saver, a user gesture, or
                // a cloak update; without this flag the system refuses to wake a stopped app on
                // some OEM builds, producing a silent bind-failure that surfaces as the opaque
                // "manager not reachable" toast. Safe to use even when the manager is not
                // stopped -- it simply has no effect on a running app.
                .addFlags(Intent.FLAG_INCLUDE_STOPPED_PACKAGES);
        // TODO: Authentication
        deliveryLog.describeTo(intent);
        try {
            boolean ok;
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                ok = context.bindService(intent, Context.BIND_AUTO_CREATE, worker, connection);
            } else {
                var contextImplClass = context.getClass();
                var getUserMethod = contextImplClass.getMethod("getUser");
                var bindServiceAsUserMethod = contextImplClass.getDeclaredMethod(
                        "bindServiceAsUser",
                        Intent.class,
                        ServiceConnection.class,
                        int.class,
                        Handler.class,
                        UserHandle.class);
                var userHandle = (UserHandle) getUserMethod.invoke(context);
                ok = Boolean.TRUE.equals(bindServiceAsUserMethod.invoke(
                        context, intent, connection, Context.BIND_AUTO_CREATE, legacyHandler(), userHandle));
            }
            bound = ok;
            return ok;
        } catch (Throwable t) {
            Log.e(TAG, "Cannot bind the manager", t);
            bound = false;
            return false;
        }
    }

    /**
     * The thread the pre-Q bind path delivers its callbacks on, made once.
     *
     * That path takes a Handler rather than an Executor, and the binding is now re-established as often as the manager
     * comes and goes -- so a thread built per attempt would be one more looper left running for every rebind on a
     * device old enough to need this path at all.
     */
    private synchronized Handler legacyHandler() {
        if (legacyHandler == null) {
            var thread = new HandlerThread("lspatch-manager-link-legacy");
            thread.start();
            legacyHandler = new Handler(thread.getLooper());
        }
        return legacyHandler;
    }

    private void unbind() {
        if (!bound) return;
        bound = false;
        try {
            context.unbindService(connection);
        } catch (Throwable t) {
            Log.w(TAG, "Cannot release the manager binding", t);
        }
    }

    private void scheduleRebind() {
        var delay = rebindDelay;
        rebindDelay = Math.min(rebindDelay * 2, REBIND_MAX_DELAY_MS);
        worker.schedule(
                () -> {
                    if (service != null) return;
                    Log.i(TAG, "Rebinding the manager");
                    if (!bind()) scheduleRebind();
                },
                delay,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Hands a manager that has just come back everything the previous one was holding: the channel it
     * drives hot reload over, which modules this process is running, and a live binder for each of the
     * module services the hook is still using.
     *
     * <p>The module lists are asked for again because that request is what carries all three: the
     * manager records the caller's modules and pushes each module's service to its companion app while
     * answering it, and the answer is where a fresh {@code IModuleService} per module comes from. The
     * code it maps for that answer is nobody's to load -- the modules in this process were loaded long
     * ago -- so it is released rather than left to a finalizer.</p>
     */
    private void reestablish(IFrameworkService manager) {
        var channel = processChannel;
        if (channel != null) {
            try {
                manager.attachProcessChannel(channel);
            } catch (Throwable t) {
                Log.w(TAG, "Could not re-attach the process channel", t);
            }
        }
        for (boolean legacy : new boolean[] {true, false}) {
            List<LoadedModule> served;
            try {
                served = legacy ? manager.getLegacyModules() : manager.getModules();
            } catch (Throwable t) {
                Log.w(TAG, "Could not re-read the module list", t);
                continue;
            }
            if (served == null) continue;
            for (var module : served) {
                if (module == null || module.packageName == null) continue;
                var proxy = moduleServices.get(module.packageName);
                if (proxy != null) proxy.onManagerReconnected(module.service);
                LoadedModules.discard(module);
            }
            snapshot.save(served, legacy);
        }
    }

    /** Replaces each module's manager binder with the host-side proxy the framework will keep. */
    private void adopt(List<LoadedModule> served) {
        for (var module : served) {
            if (module == null || module.packageName == null) continue;
            var proxy = moduleService(module.packageName);
            proxy.setLive(module.service);
            module.service = proxy;
        }
    }

    private ReconnectingModuleService moduleService(String modulePackageName) {
        return moduleServices.computeIfAbsent(modulePackageName, pkg -> new ReconnectingModuleService(pkg, stateDir));
    }

    private List<LoadedModule> modules(boolean legacy) {
        var manager = service;
        if (manager != null) {
            try {
                var served = legacy ? manager.getLegacyModules() : manager.getModules();
                if (served != null) {
                    adopt(served);
                    // Off this thread: the app's own start is waiting on this call, and recording what
                    // was served is for the next launch rather than for this one.
                    worker.execute(() -> snapshot.save(served, legacy));
                    return served;
                }
            } catch (Throwable t) {
                Log.w(TAG, "The manager could not serve this app's modules", t);
            }
        }
        var restored = snapshot.restore(legacy, this::moduleService);
        // Said out loud, because this is the one path where nobody else can say it: the manager did not
        // answer, so the count of what was loaded anyway exists only here.
        Log.i(TAG, "Serving " + restored.size() + (legacy ? " legacy" : "") + " module(s) from the snapshot");
        return restored;
    }

    private void toast(String message) {
        try {
            Toast.makeText(context, message, Toast.LENGTH_SHORT).show();
        } catch (Throwable t) {
            // No looper on this thread, or a context that cannot show one: the log line is the report.
            Log.w(TAG, message);
        }
    }

    @Override
    public boolean isLogMuted() throws RemoteException {
        var manager = service;
        return manager != null && manager.isLogMuted();
    }

    @Override
    public List<LoadedModule> getLegacyModules() {
        return modules(true);
    }

    @Override
    public List<LoadedModule> getModules() {
        return modules(false);
    }

    @Override
    public String getPrefsPath(String packageName) {
        return new File(Environment.getDataDirectory(), "data/" + packageName + "/shared_prefs/").getAbsolutePath();
    }

    @Override
    public ParcelFileDescriptor openManagerApk() throws RemoteException {
        var manager = service;
        return manager == null ? null : manager.openManagerApk();
    }

    @Override
    public IBinder requestManagerService() {
        return null;
    }

    @Override
    public void attachProcessChannel(IProcessChannel channel) {
        // The manager drives hot reload but is a plain app, so the framework's own channel -- which
        // gates on the system uid -- would refuse it. Hand the manager an LSPatch channel that runs the
        // in-process swap for it instead; the framework's channel is unused without a daemon.
        var ours = new LSPatchProcessChannel();
        processChannel = ours;
        var manager = service;
        if (manager == null) return;
        try {
            manager.attachProcessChannel(ours);
        } catch (Throwable t) {
            Log.w(TAG, "Could not attach the process channel", t);
        }
    }

    @Override
    public IBinder asBinder() {
        return token;
    }
}
