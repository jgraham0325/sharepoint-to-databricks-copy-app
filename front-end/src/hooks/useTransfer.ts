import { useState, useCallback, useRef } from "react";
import {
  startTransfer,
  getTransferStatus,
  type TransferRequest,
  type TransferState,
} from "../api/transfer";
import toast from "react-hot-toast";

export function useTransfer() {
  const [transferState, setTransferState] = useState<TransferState | null>(
    null
  );
  const [isTransferring, setIsTransferring] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  const beginTransfer = useCallback(
    async (req: TransferRequest) => {
      setIsTransferring(true);
      try {
        const state = await startTransfer(req);
        setTransferState(state);

        // Poll for status
        pollRef.current = setInterval(async () => {
          try {
            const updated = await getTransferStatus(state.transfer_id);
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
          } catch {
            stopPolling();
            setIsTransferring(false);
          }
        }, 2000);
      } catch (err: any) {
        toast.error(err.message);
        setIsTransferring(false);
      }
    },
    [stopPolling]
  );

  return { transferState, isTransferring, beginTransfer };
}
