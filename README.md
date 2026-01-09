# A-QPSK-Tranceiver-Receiver-System-Based-on-PlutoSDR
A simple QPSK digital tranceiver/receiver system based on PlutoSDR and GNURadio, which is theoretically compatible to any binary-coded files.

This repository presents a QPSK-based digital file transmission system implemented in GNU Radio Companion. The system is designed to transmit arbitrary binary files (e.g., `.jpg`, `.txt`, `.pdf`, `.docx`) over a QPSK physical layer, and to reassemble the original file reliably at the receiver side. Several .jpg files and .docx files have been tested in Win10 & GNURadio V2025.03.14 (RadioCompanion). Since no hash test module is added, there may be error when transmitting larger files.
## System Architecture

The system consists of two main components:

* **TX (Transmitter):**
  Encapsulates file data into framed packets, adds metadata headers, applies QPSK modulation, and transmits via SDR. An ADI PlutoSDR (AD9363) was used in experiment for this part. 

* **RX (Receiver):**
  Demodulates the QPSK signal, verifies packet integrity, scans the stream for file metadata, and reassembles the original file. A Microphase ANTSDR-E316 (AD9361, PlutoSDR firmware) was tested.

---

## Flowgraph Overview

### Transmitter (TX)

The TX flowgraph converts a file into framed packets and transmits them using QPSK modulation. 

![TX Flowgraph](docs/qpsk_tranceiver.png)

---

### Receiver (RX)

The RX flowgraph demodulates the signal, extracts framed data, and reconstructs the transmitted file from the byte stream. 

![RX Flowgraph](docs/qpsk_receiver.png)

---

## Reference 

This project is inspired by and built upon the open-source work of **Smith**, particularly his QPSK-based video transmission system using AD936x/PlutoSDR.
You can find his original project file at:
```
  https://www.kechuang.org/t/91540
```
As well as his demo video on Bilibili:
```
https://www.bilibili.com/video/BV1DBkQBiECd/?share_source=copy_web&vd_source=27407fbbb1ed6821f03618df1e2d03eb](https://www.bilibili.com/video/BV1DBkQBiECd/?share_source=copy_web&vd_source=27407fbbb1ed6821f03618df1e2d03eb
```
Smith’s work provided the foundational physical-layer structure for QPSK framing, synchronization, and streaming.
This repository extends the original concept from **real-time TS video streaming** to **general-purpose file transmission with metadata-aware reconstruction**.

---

## Usage

1. Configure the **TX flowgraph** and set the target file path in the *Meta+File Source* block.
2. Start the **RX flowgraph** and specify the output directory.
3. Run both sides. After successful reception, the reconstructed file will appear in the RX output folder.

> For large files or unstable channels, ensure proper synchronization and CRC settings to avoid partial reconstruction.
