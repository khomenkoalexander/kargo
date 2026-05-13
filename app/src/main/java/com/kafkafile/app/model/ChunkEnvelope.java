package com.kafkafile.app.model;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Binary chunk envelope format (69-byte header + payload):
 *
 *  0-15  : transferId (UUID MSB + LSB, 16 bytes)
 * 16-23  : byteOffset (long, 8 bytes)
 * 24-27  : seq        (int,  4 bytes)
 * 28-31  : length     (int,  4 bytes)
 * 32-35  : crc32      (int,  4 bytes)
 *    36  : flags      (byte; bit 0 = EOF)
 * 37-68  : sha256     (32 bytes; zero unless EOF)
 * 69+    : payload    (N bytes)
 */
public class ChunkEnvelope {

    public static final int HEADER_SIZE = 69;

    private UUID transferId;
    private long byteOffset;
    private int seq;
    private int length;
    private int crc32;
    private boolean eof;
    private byte[] sha256 = new byte[32];
    private byte[] payload;

    public static byte[] encode(UUID transferId, long byteOffset, int seq,
                                byte[] payload, boolean eof, byte[] sha256) {
        byte[] result = new byte[HEADER_SIZE + payload.length];
        ByteBuffer buf = ByteBuffer.wrap(result);

        buf.putLong(transferId.getMostSignificantBits());
        buf.putLong(transferId.getLeastSignificantBits());
        buf.putLong(byteOffset);
        buf.putInt(seq);
        buf.putInt(payload.length);

        CRC32 crc = new CRC32();
        crc.update(payload);
        buf.putInt((int) crc.getValue());

        buf.put((byte) (eof ? 1 : 0));

        if (sha256 != null && sha256.length == 32) {
            buf.put(sha256);
        } else {
            buf.put(new byte[32]);
        }

        buf.put(payload);
        return result;
    }

    public static ChunkEnvelope decode(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Data too short for chunk header: " + data.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(data);

        ChunkEnvelope e = new ChunkEnvelope();
        long msb = buf.getLong();
        long lsb = buf.getLong();
        e.transferId = new UUID(msb, lsb);
        e.byteOffset = buf.getLong();
        e.seq = buf.getInt();
        e.length = buf.getInt();
        e.crc32 = buf.getInt();
        e.eof = (buf.get() & 0x01) == 1;
        e.sha256 = new byte[32];
        buf.get(e.sha256);
        e.payload = new byte[data.length - HEADER_SIZE];
        buf.get(e.payload);

        CRC32 crc = new CRC32();
        crc.update(e.payload);
        if ((int) crc.getValue() != e.crc32) {
            throw new IllegalArgumentException("CRC32 mismatch for seq=" + e.seq);
        }

        return e;
    }

    public UUID getTransferId() { return transferId; }
    public long getByteOffset() { return byteOffset; }
    public int getSeq() { return seq; }
    public int getLength() { return length; }
    public int getCrc32() { return crc32; }
    public boolean isEof() { return eof; }
    public byte[] getSha256() { return sha256; }
    public byte[] getPayload() { return payload; }
}
