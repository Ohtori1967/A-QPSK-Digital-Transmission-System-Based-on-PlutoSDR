import os
import struct
import numpy as np
from gnuradio import gr

class blk(gr.basic_block):
    """
    File Reassembler (scan-stream, single-shot):
    - Scan byte stream for magic b'FILE'
    - Parse meta_len / name_len / file_size / name
    - Then write exactly file_size bytes after the meta
    - Write to .part and rename on completion
    - After done: drop all bytes (safe to stop anytime)
    """

    def __init__(self, out_dir="C:/tmp/out", debug=True, overwrite=True, max_buffer=4*1024*1024):
        gr.basic_block.__init__(self, name="File Reassembler (scan-stream)", in_sig=[np.uint8], out_sig=None)
        self.out_dir = out_dir
        self.debug = bool(debug)
        self.overwrite = bool(overwrite)
        self.max_buffer = int(max_buffer)

        os.makedirs(self.out_dir, exist_ok=True)

        self.state = "SCAN"
        self.buf = bytearray()

        self.meta_len = None
        self.file_size = None
        self.file_name = None

        self.fh = None
        self.part_path = None
        self.final_path = None
        self.written = 0
        self.done = False

    def _log(self, s):
        if self.debug:
            print("[REASM]", s)

    def _close(self):
        try:
            if self.fh:
                self.fh.flush()
                self.fh.close()
        except Exception:
            pass
        self.fh = None

    def _open_out(self, name, size):
        safe = os.path.basename(name) if name else "recv.bin"
        final = os.path.join(self.out_dir, safe)
        part  = final + ".part"

        if (not self.overwrite) and os.path.exists(final):
            base, ext = os.path.splitext(final)
            k = 1
            while os.path.exists(f"{base}_{k}{ext}") or os.path.exists(f"{base}_{k}{ext}.part"):
                k += 1
            final = f"{base}_{k}{ext}"
            part  = final + ".part"

        self.final_path = final
        self.part_path  = part

        self._close()
        self.fh = open(self.part_path, "wb")
        self.written = 0
        self._log(f"OPEN {self.part_path} (expect {size} bytes)")

    def _finish(self):
        self._close()
        try:
            if os.path.exists(self.final_path):
                os.remove(self.final_path)
        except Exception:
            pass
        os.replace(self.part_path, self.final_path)
        self._log(f"DONE {self.final_path} (written={self.written})")

    def _try_parse_meta_at(self, idx):
        # need at least fixed 16 bytes header
        if len(self.buf) < idx + 16:
            return False

        if self.buf[idx:idx+4] != b"FILE":
            return False

        ver = self.buf[idx+4]
        meta_len = struct.unpack("<H", self.buf[idx+5:idx+7])[0]
        name_len = self.buf[idx+7]
        fsize = struct.unpack("<Q", self.buf[idx+8:idx+16])[0]

        # sanity checks to reduce false positive
        if ver != 1:
            return False
        if meta_len < 16 or meta_len > 4096:
            return False
        if name_len > 255:
            return False
        if fsize <= 0 or fsize > (1024**3):  # 1GB cap
            return False

        if len(self.buf) < idx + meta_len:
            return False  # wait more bytes

        name_end = idx + 16 + name_len
        if name_end > idx + meta_len:
            return False

        name = bytes(self.buf[idx+16:name_end]).decode("utf-8", errors="ignore").strip("\x00")
        if not name:
            name = "recv.bin"

        self.meta_len = meta_len
        self.file_size = fsize
        self.file_name = name

        self._log(f"META ok @ {idx}: ver={ver}, meta_len={meta_len}, name={name}, size={fsize}")
        return True

    def general_work(self, input_items, output_items):
        inp = input_items[0]
        n = len(inp)

        if self.done:
            self.consume(0, n)
            return 0

        # append to buffer
        self.buf += bytes(inp)

        # prevent runaway memory if no sync
        if len(self.buf) > self.max_buffer:
            # keep last 1MB
            self.buf = self.buf[-1024*1024:]

        if self.state == "SCAN":
            # search for 'FILE'
            # we search all occurrences, pick the first that passes sanity + full meta present
            start = 0
            while True:
                idx = self.buf.find(b"FILE", start)
                if idx < 0:
                    break
                if self._try_parse_meta_at(idx):
                    # consume bytes before meta start
                    if idx > 0:
                        del self.buf[:idx]
                    # now buf starts with FILE...
                    # consume full meta
                    del self.buf[:self.meta_len]
                    self._open_out(self.file_name, self.file_size)
                    self.state = "RECV"
                    break
                start = idx + 1

        if self.state == "RECV" and self.fh is not None:
            remain = self.file_size - self.written
            if remain > 0 and len(self.buf) > 0:
                take = min(remain, len(self.buf))
                chunk = self.buf[:take]
                self.fh.write(chunk)
                self.written += take
                del self.buf[:take]

            if self.written >= self.file_size:
                self._finish()
                self.done = True
                self.state = "DONE"

        self.consume(0, n)
        return 0

    def stop(self):
        self._close()
        return True
