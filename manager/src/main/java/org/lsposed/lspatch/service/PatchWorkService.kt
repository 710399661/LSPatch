package org.lsposed.lspatch.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import android.util.Log
import androidx.core.app.NotificationCompat
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch
import org.lsposed.lspatch.R
import org.lsposed.lspatch.data.model.PatchStage
import org.lsposed.lspatch.data.model.PatchStep
import org.lsposed.lspatch.data.repository.PatchJobHost
import org.lsposed.lspatch.ui.activity.MainActivity

/**
 * Foreground service that rides alongside the single running [PatchJobHost] job.
 *
 * The patch job itself is hosted by the application scope in [PatchJobHost], because the job must
 * outlive the patch screen ViewModel the person pressed "Patch" from. That, however, leaves the
 * manager process in Android's ordinary cached-app bucket for the whole of the slow step — writing
 * and signing a 200 MB+ patched APK — so OEMs such as Xiaomi (SmartPower / PowerInsight) and the
 * AOSP cached-app killer reap the manager the moment the person navigates away. When the process
 * is reaped mid-write the patched APK is left half-written on disk; the user sees "LSPatch stopped
 * in the background" and the only useful signal is the missing output file.
 *
 * This service therefore:
 *  1. Promotes the process to the DATA_SYNC foreground bucket for the duration of the job,
 *  2. Holds a PARTIAL_WAKE_LOCK so the CPU does not deep-sleep mid write,
 *  3. Holds a WIFI_MODE_FULL_HIGH_PERF Wi-Fi lock when a source APK comes from the network,
 *  4. Mirrors [PatchJobHost.step] into a user-visible notification with the per-stage progress,
 *  5. Exits cleanly whenever [PatchJobHost.busy] becomes false again.
 *
 * A separate service from [ManagerResidentService] is deliberate: the resident service needs to
 * stay alive for hours or days, while this one's lifecycle is exactly one patch/install/restore
 * run, so that when the job is done the manager can drop back to the low-importance resident
 * notification instead of looking permanently "busy".
 */
class PatchWorkService : Service() {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private lateinit var wakeLock: PowerManager.WakeLock
    private var wifiLock: WifiManager.WifiLock? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        createChannel()

        val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
        wakeLock =
            pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "LSPatch:PatchWork").apply {
                setReferenceCounted(false)
                // Force a wakelock timeout just in case stopSelf() is never reached (e.g. the
                // process is killed by the shell watchdog running its own respawn path). 30 minutes
                // is comfortably longer than the worst-case patching + reinstall of a split app,
                // but short enough that a forgotten lock cannot drain a battery overnight.
                acquire(WAKE_LOCK_TIMEOUT_MS)
            }

        // A source apk chosen from storage might actually live on a content:// provider served over
        // Wi-Fi (DocumentsUI → SMB / cloud). Best-effort only: missing permission is harmless.
        runCatching {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
            @Suppress("DEPRECATION")
            wifiLock =
                wm.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "LSPatch:PatchWork").apply {
                    setReferenceCounted(false)
                    acquire()
                }
        }

        startForegroundNow(buildNotification(PatchJobHost.step.value))

        // collectLatest: when the step churns (many .Running emissions inside the same stage) only
        // the latest one is pushed into the notification, avoiding a notification-service queue
        // backing up behind the 100+ stage ticks the patcher emits during a 14 s run.
        scope.launch {
            PatchJobHost.step.collectLatest { step ->
                runCatching {
                    val nm = getSystemService(NotificationManager::class.java)
                    nm.notify(NOTIF_ID, buildNotification(step))
                }
                if (!PatchJobHost.busy) {
                    // Idle / Patched / Failed / Done: the job is no longer running. Hand back the
                    // resources and tear the service down the moment the caller has left us.
                    stopForeground(STOP_FOREGROUND_REMOVE)
                    stopSelf()
                }
            }
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Not START_STICKY: if the system kills the service despite the FGS, the app's own
        // process will be reaped along with it, and PatchJobHost's in-memory job is lost anyway —
        // there is nothing to resume on restart. A fresh start through the UI is correct.
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        scope.cancel()
        if (wakeLock.isHeld) runCatching { wakeLock.release() }
        wifiLock?.takeIf { it.isHeld }?.let { runCatching { it.release() } }
        super.onDestroy()
    }

    // ---- Foreground plumbing ---------------------------------------------------------------

    private fun startForegroundNow(notification: Notification) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(
                NOTIF_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC,
            )
        } else {
            startForeground(NOTIF_ID, notification)
        }
    }

    private fun stageLabel(stage: PatchStage?): String =
        when (stage) {
            PatchStage.ReadingApk -> getString(R.string.patch_stage_reading)
            PatchStage.SigningSetup -> getString(R.string.patch_stage_signing)
            PatchStage.RewritingManifest -> getString(R.string.patch_stage_rewriting)
            PatchStage.InjectingLoader -> getString(R.string.patch_stage_injecting)
            PatchStage.EmbeddingModules -> getString(R.string.patch_stage_embedding)
            PatchStage.PackingSplit -> getString(R.string.patch_stage_packing_split)
            PatchStage.WritingAndSigning -> getString(R.string.patch_stage_writing)
            PatchStage.Finished -> getString(R.string.patch_patched)
            null -> ""
        }

    private fun buildNotification(step: PatchStep): Notification {
        val (titleRes, label) =
            when (step) {
                is PatchStep.Preparing -> R.string.patch_work_title_patching to step.request.label
                is PatchStep.Running -> R.string.patch_work_title_patching to step.request.label
                is PatchStep.Patched -> R.string.patch_work_title_installing to step.request.label
                is PatchStep.Installing -> R.string.patch_work_title_installing to step.packageName
                is PatchStep.Uninstalling -> R.string.patch_work_title_uninstalling to step.packageName
                is PatchStep.NeedsUninstall -> R.string.patch_work_title_installing to step.request.label
                is PatchStep.Restoring -> R.string.patch_work_title_restoring to step.label
                is PatchStep.Failed -> R.string.patch_work_title_patching to step.title
                is PatchStep.Done -> R.string.patch_work_title_installing to step.packageName
                PatchStep.Idle -> R.string.patch_work_title_patching to getString(R.string.app_name)
            }

        val title = getString(titleRes, label)
        val content = getString(R.string.patch_work_text)

        val builder =
            NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle(title)
                .setTicker(title)
                .setContentText(content)
                .setSmallIcon(R.mipmap.ic_launcher)
                .setOngoing(true)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .setCategory(Notification.CATEGORY_PROGRESS)
                .setShowWhen(true)
                // Tapping the notification returns the person to the manager — they have the patch
                // screen's progress lines to inspect, so route them there rather than to the
                // launcher-home of the app.
                .setContentIntent(tapPendingIntent())
                .setForegroundServiceBehavior(NotificationCompat.FOREGROUND_SERVICE_IMMEDIATE)

        when (step) {
            is PatchStep.Running -> {
                val stage = stageLabel(step.stage)
                val progressText =
                    if (stage.isNotBlank())
                        getString(R.string.patch_work_stage, stage, step.apkIndex, step.apkCount)
                    else null
                val indeterminate = step.apkCount <= 0
                if (!indeterminate) {
                    builder.setProgress(step.apkCount, step.apkIndex, false)
                } else {
                    builder.setProgress(0, 0, true)
                }
                if (progressText != null) builder.setStyle(
                    NotificationCompat.BigTextStyle()
                        .setBigContentTitle(title)
                        .bigText(progressText)
                )
            }
            is PatchStep.Preparing -> builder.setProgress(0, 0, true)
            is PatchStep.Installing,
            is PatchStep.Uninstalling,
            is PatchStep.Restoring,
            is PatchStep.Confirming -> builder.setProgress(0, 0, true)
            else -> Unit
        }

        return builder.build()
    }

    private fun tapPendingIntent(): PendingIntent? {
        val open = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
        }
        val flags =
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M)
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            else PendingIntent.FLAG_UPDATE_CURRENT
        return PendingIntent.getActivity(this, 0, open, flags)
    }

    private fun createChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel =
                NotificationChannel(
                    CHANNEL_ID,
                    getString(R.string.patch_work_channel),
                    // LOW: the in-app Patch screen shows full progress; the notification's job is
                    // to keep the system honest, not to drive the user's attention. LOW keeps the
                    // status-bar icon present without peeking or vibrating.
                    NotificationManager.IMPORTANCE_LOW,
                ).apply {
                    setShowBadge(false)
                    enableVibration(false)
                    enableLights(false)
                }
            getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
        }
    }

    companion object {
        private const val TAG = "LSPatch-PatchWork"
        private const val CHANNEL_ID = "lspatch_patch_work"
        private const val NOTIF_ID = 0x1101
        private const val WAKE_LOCK_TIMEOUT_MS = 30L * 60L * 1000L

        /**
         * Starts the foreground service tracking the current [PatchJobHost.busy] run.
         *
         * Called *before* a patch/install/restore coroutine is launched, not from inside it: the
         * system gives the app a 5 s window after a user-initiated tap to startForeground() for
         * real, and launching the coroutine inside the service connection's after-commit callback
         * risks missing the window. Launching the service first and letting it bind itself to
         * [PatchJobHost.step] keeps the FGS lifecycle firmly inside the post-tap window.
         */
        fun ensureStarted(context: Context) {
            val ctx = context.applicationContext
            val intent = Intent(ctx, PatchWorkService::class.java)
            runCatching {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    ctx.startForegroundService(intent)
                } else {
                    ctx.startService(intent)
                }
            }.onFailure { t ->
                // Starting a foreground service from background is restricted on Android 12+. The
                // patch job's own UI thread is still visible when this is called (caller is a
                // button press in the app), so this should never fire; if it does, the resident
                // service is still a last-resort FG promotion, so the patch job still has *some*
                // protection rather than none.
                Log.w(TAG, "Could not start patch-work foreground service", t)
            }
        }
    }
}
