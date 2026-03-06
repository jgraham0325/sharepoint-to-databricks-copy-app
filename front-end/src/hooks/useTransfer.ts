import { useState, useCallback, useRef, useEffect } from "react";
import {
  startTransfer,
  getTransferStatus,
  type TransferRequest,
  type TransferState,
} from "../api/transfer";
import toast from "react-hot-toast";

const POLL_INTERVAL_MS = 2000;

function isNotFoundError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return msg.includes("404") || msg.toLowerCase().includes("not found");
}

export function useTransfer(
  initialTransferId: string | null,
  onTransferNotFound?: () => void
) {
  const [transferState, setTransferState] = useState<TransferState | null>(
    null
  );
  const [isTransferring, setIsTransferring] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isPollingRef = useRef(false);

  const stopPolling = useCallback(() => {
    if (timeoutRef.current !== null) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
    isPollingRef.current = false;
  }, []);

  const pollOnce = useCallback(
    (transferId: string) => {
      if (!isPollingRef.current) return;
      getTransferStatus(transferId)
        .then((updated) => {
          if (!isPollingRef.current) return;
          setTransferState(updated);
          if (
            updated.status === "completed" ||
            updated.status === "failed"
          ) {
            stopPolling();
            setIsTransferring(false);
            if (updated.status === "completed") {
              toast.success(
                `Transferred ${updated.completed} of ${updated.total} files`
              );
            } else {
              toast.error(
                `Transfer finished with ${updated.failed} failures`
              );
            }
            return;
          }
          timeoutRef.current = setTimeout(
            () => pollOnce(transferId),
            POLL_INTERVAL_MS
          );
        })
        .catch((err) => {
          stopPolling();
          setIsTransferring(false);
          if (isNotFoundError(err)) {
            setTransferState(null);
            toast.error(
              "Transfer status no longer available (server may have restarted)."
            );
            onTransferNotFound?.();
          }
        });
    },
    [stopPolling, onTransferNotFound]
  );

  const startPolling = useCallback(
    (transferId: string) => {
      stopPolling();
      isPollingRef.current = true;
      timeoutRef.current = setTimeout(
        () => pollOnce(transferId),
        POLL_INTERVAL_MS
      );
    },
    [stopPolling, pollOnce]
  );

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      stopPolling();
    };
  }, [stopPolling]);

  // Rehydrate from initialTransferId (URL or sessionStorage)
  useEffect(() => {
    if (!initialTransferId) return;
    if (transferState?.transfer_id === initialTransferId) {
      return; // already have this state (and polling if in progress)
    }

    let cancelled = false;
    getTransferStatus(initialTransferId)
      .then((state) => {
        if (cancelled) return;
        setTransferState(state);
        if (
          state.status === "in_progress" ||
          state.status === "pending"
        ) {
          setIsTransferring(true);
          startPolling(initialTransferId);
        } else {
          setIsTransferring(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setIsTransferring(false);
          if (isNotFoundError(err)) {
            setTransferState(null);
            toast.error(
              "Transfer status no longer available (server may have restarted)."
            );
            onTransferNotFound?.();
          }
        }
      });

    return () => {
      cancelled = true;
    };
  }, [initialTransferId, transferState?.transfer_id, startPolling, onTransferNotFound]);

  // Page Visibility: when tab becomes visible, refresh immediately if we're polling
  useEffect(() => {
    const handleVisibility = () => {
      if (
        document.visibilityState === "visible" &&
        isPollingRef.current &&
        transferState?.transfer_id
      ) {
        getTransferStatus(transferState.transfer_id)
          .then((updated) => {
            setTransferState(updated);
            if (
              updated.status === "completed" ||
              updated.status === "failed"
            ) {
              stopPolling();
              setIsTransferring(false);
              if (updated.status === "completed") {
                toast.success(
                  `Transferred ${updated.completed} of ${updated.total} files`
                );
              } else {
                toast.error(
                  `Transfer finished with ${updated.failed} failures`
                );
              }
            }
          })
          .catch((err) => {
            if (isNotFoundError(err)) {
              setTransferState(null);
              stopPolling();
              setIsTransferring(false);
              toast.error(
                "Transfer status no longer available (server may have restarted)."
              );
              onTransferNotFound?.();
            }
          });
      }
    };
    document.addEventListener("visibilitychange", handleVisibility);
    return () => document.removeEventListener("visibilitychange", handleVisibility);
  }, [transferState?.transfer_id, stopPolling, onTransferNotFound]);

  const beginTransfer = useCallback(
    async (req: TransferRequest) => {
      setIsTransferring(true);
      try {
        const state = await startTransfer(req);
        setTransferState(state);
        startPolling(state.transfer_id);
      } catch (err: any) {
        toast.error(err.message);
        setIsTransferring(false);
      }
    },
    [startPolling]
  );

  return { transferState, isTransferring, beginTransfer };
}
