import os
import struct
import numpy as np
from gnuradio import gr
import pmt

class blk(gr.basic_block):
    """
    Tagged Meta+File Source (fixed-size packets)
    Output packets of exactly meta_len bytes (default 512),
    with a length tag at the start of every packet.

    Packet order: META(512) then FILE-DATA(512 blocks, zero-padded on last),
    repeat: META + FILE + META + FILE ...
    """

    def __init__(self, filepath="C:/tmp/test.jpg", meta_len=512, repeat=True, len_tag_key="packet_len"):
        gr.basic_block.__init__(self, name="Meta+File Source (tagged)", in_sig=None, out_sig=[np.uint8])

        self.filepath = filepath
        self.meta_len = int(meta_len)
        self.repeat = bool(repeat)
        self.len_tag_key = str(len_tag_key)

        if self.meta_len < 16:
            raise ValueError("meta_len must be >= 16")

        self._meta = self._build_meta(self.filepath, self.meta_len)
        self._fh = None
        self._open_file()

        self._phase = "meta"   # "meta" -> "file"
        self._file_bytes_left = self._get_size()

        # 给 tag 用
        self._tag_key = pmt.intern(self.len_tag_key)

    def _get_size(self):
        try:
            return int(os.path.getsize(self.filepath))
        except Exception:
            return 0

    def _build_meta(self, filepath, meta_len):
        fname = os.path.basename(filepath)
        name_b = fname.encode("utf-8", errors="ignore")[:255]
        name_len = len(name_b)
        fsize = self._get_size()

        header = bytearray()
        header += b"FILE"                        # magic
        header += struct.pack("<B", 1)          # version
        header += struct.pack("<H", int(meta_len))
        header += struct.pack("<B", int(name_len))
        header += struct.pack("<Q", int(fsize))
        header += name_b

        if len(header) < meta_len:
            header += b"\x00" * (meta_len - len(header))
        else:
            header = header[:meta_len]

        return bytes(header)

    def _open_file(self):
        try:
            if self._fh:
                self._fh.close()
        except Exception:
            pass
        try:
            self._fh = open(self.filepath, "rb")
        except Exception:
            self._fh = None

    def forecast(self, noutput_items, ninput_items_required):
        return

    def general_work(self, input_items, output_items):
        out = output_items[0]
        n = len(out)

        pkt = self.meta_len
        n_pkts = n // pkt
        if n_pkts <= 0:
            self.produce(0, 0)
            return gr.WORK_CALLED_PRODUCE

        produced = 0
        for _ in range(n_pkts):
            start = produced
            end = start + pkt

            self.add_item_tag(0, self.nitems_written(0) + start, self._tag_key, pmt.from_long(pkt))

            if self._phase == "meta":
                out[start:end] = np.frombuffer(self._meta, dtype=np.uint8)
                self._file_bytes_left = self._get_size()
                self._open_file()
                self._phase = "file"

            else:
                # file phase: 读 pkt 字节，不足补 0
                buf = b""
                if self._fh is not None and self._file_bytes_left > 0:
                    to_read = min(pkt, self._file_bytes_left)
                    buf = self._fh.read(to_read)
                    self._file_bytes_left -= len(buf)

                if len(buf) < pkt:
                    buf = buf + (b"\x00" * (pkt - len(buf)))

                out[start:end] = np.frombuffer(buf, dtype=np.uint8)

                if self._file_bytes_left <= 0:
                    if self.repeat:
                        self._meta = self._build_meta(self.filepath, self.meta_len)
                        self._phase = "meta"
                    else:
                        self._phase = "zeros"

            if self._phase == "zeros":
                out[start:end] = 0

            produced += pkt

        self.produce(0, produced)
        return gr.WORK_CALLED_PRODUCE
