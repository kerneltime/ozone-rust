# EC byte-equivalence with Apache Ozone's Java coder

This note records *why* the Rust erasure coder produces byte-identical output to
Apache Ozone's Java EC, and how that is proven. The proof is in Rust and needs no
JVM; an optional JVM cross-check is described at the end.

## Verdict

The Rust encoder (Intel ISA-L `gf_gen_cauchy1_matrix` + `ec_init_tables` +
`ec_encode_data`) is **byte-identical** to Ozone's EC parity output for both of
Ozone's coders:

- **Native coder (Ozone's default):** `NativeRSRawEncoder` binds, via Hadoop
  3.4.3 JNI, to *the same ISA-L library* this project links. Same library, same
  Cauchy matrix, same GF kernel → identical bytes by construction.
- **Pure-Java fallback (`RSRawEncoder`):** builds its generator matrix with
  `RSUtil.genCauchyMatrix` — identity rows, then parity rows
  `a[pos] = GF256.gfInv(i ^ j)` over GF(2^8) with primitive polynomial
  **285 (0x11D)**. Its source comment states it is "Ported from Intel ISA-L
  library." This is the *same* Cauchy construction ISA-L uses.

Both Ozone paths and ISA-L use the identical field (poly 285) and the identical
matrix formula, so the generator matrices are equal and the encoded parity is
byte-identical.

### Evidence (Ozone source, read at `~/ozone-src`)

- `hadoop-hdds/erasurecode/.../rawcoder/util/RSUtil.java` — `genCauchyMatrix`:
  identity rows + `GF256.gfInv((byte)(i ^ j))`.
- `hadoop-hdds/erasurecode/.../rawcoder/util/GaloisField.java` —
  `DEFAULT_PRIMITIVE_POLYNOMIAL = 285`.
- `hadoop-hdds/erasurecode/.../rawcoder/CodecRegistry.java` — native coder is
  registered at index 0 (default), pure-Java is the fallback.
- `hadoop-hdds/erasurecode/.../rawcoder/ErasureCodeNative.java` — native path
  loads ISA-L via `libhadoop` (`buildSupportsIsal`).

## The proof (no JVM required)

`crates/isa-l-safe/tests` / `src/lib.rs`:

1. `matrix_byte_identical_to_ozone_java_cauchy` (unit test) ports Ozone's exact
   `genCauchyMatrix` algorithm — including a from-scratch GF(2^8) inverse with
   polynomial 0x11D — and asserts the result equals ISA-L's
   `gf_gen_cauchy1_matrix` for RS-3-2, RS-6-3, RS-10-4. Identical matrices over
   the identical field ⇒ identical parity.
2. `tests/golden_vectors.rs` pins the exact parity bytes ISA-L produces for the
   three profiles on a deterministic input (shard `i`, byte `j` =
   `(i*37 + j*5 + 1) mod 256`, `len = 32`). These bytes are therefore also
   Ozone's bytes, and they guard against any future regression.

## Stripe layout note (partial last stripe)

The *matrix/GF* equivalence above covers raw `k`-data → `p`-parity encoding and
every full stripe. One layout detail differs and is a deliberate, documented
choice:

Ozone sizes a *partial* last stripe's parity cells to
`parityCellSize = firstDataCell.position()` (the largest data cell in that
stripe, ≤ `ecChunkSize`), padding shorter data cells with zeros to that size
before encoding (`ECKeyOutputStream.generateParityCells`). This project's
`ozone-ec::stripe` stores parity cells at the full `chunk_size` for internal
simplicity. The first `parityCellSize` bytes are byte-identical to Ozone (the
remaining columns encode all-zero padding, so they are deterministic); only the
*stored length* of a partial-stripe parity cell differs.

This matters only for cross-implementation chunk-file interop, which is a
non-goal (greenfield datanode, no upgrade/migration requirement). The Rust
encode/decode is self-consistent and fuzz-verified
(`ozone-ec::stripe::encode_then_decode_survives_any_p_erasures`). To make a
partial-stripe parity cell byte-identical on disk, truncate it to
`parityCellSize`; the leading bytes already match.

## Optional JVM cross-check

To independently confirm against a running Ozone build, compile a dumper against
`hdds-erasurecode` that feeds the same deterministic input and prints parity
hex, then diff against the constants in `golden_vectors.rs`:

```java
// classpath: hadoop-hdds/erasurecode (hdds-erasurecode) + hadoop-common 3.4.3
import org.apache.ozone.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import java.nio.ByteBuffer;

public final class OzoneEcVectorDumper {
  static byte[][] data(int k, int len) {
    byte[][] d = new byte[k][len];
    for (int i = 0; i < k; i++)
      for (int j = 0; j < len; j++)
        d[i][j] = (byte) ((i * 37 + j * 5 + 1) & 0xff);
    return d;
  }
  static void dump(int k, int p, int len) throws Exception {
    RSRawEncoder enc = new RSRawEncoder(new ErasureCoderOptions(k, p));
    ByteBuffer[] in = new ByteBuffer[k], out = new ByteBuffer[p];
    for (int i = 0; i < k; i++) { in[i] = ByteBuffer.allocateDirect(len); in[i].put(data(k, len)[i]); in[i].flip(); }
    for (int i = 0; i < p; i++) out[i] = ByteBuffer.allocateDirect(len);
    enc.encode(in, out);
    StringBuilder sb = new StringBuilder("rs-" + k + "-" + p + ": ");
    for (int i = 0; i < p; i++) { for (int b = 0; b < len; b++) sb.append(String.format("%02x", out[i].get(b))); if (i + 1 < p) sb.append(','); }
    System.out.println(sb);
  }
  public static void main(String[] a) throws Exception { dump(3,2,32); dump(6,3,32); dump(10,4,32); }
}
```

Building Ozone's Java tree (Maven + a JDK) is the only prerequisite; the Rust
proof above already establishes the result, so this is supplementary.
