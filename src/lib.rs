/*!
A SPSC ring buffer library with multi-channel buffers and capabilities to
retain readable data.
## Development Status
wreath is early in development.
There is still testing to be done.
Breaking API changes are to be expected.

## Example
```rust
use wreath::ring_buf;
use wreath::Reader;
use wreath::Writer;

// Create a ring buffer with a read capacity of 4 and a write capacity of 8.
let (reader, writer) = ring_buf::<usize>(4, 8);

let writes = writer.real_writes_available();
for i in 0..writes {
    writer.write_forward(i, i);
}
writer.advance_write_position_by(writes);

let reads = reader.real_reads_available();
for i in 0..reads {
    let value = reader.read_forward(i);
    assert_eq!(i, value);
}
reader.advance_read_position_by(reads);

```

## Safety
All data that wreath allocates within its ring buffers is initialized
and indexes always wrap around the buffer. Incorrectly indexing or advancing
read/write positions may produce unexpected results, but should not produce
undefined behavior. Therefore, wreath allows for unchecked indexing and
advancing of positions without the need for `unsafe` blocks.
*/
mod buf;
mod reader;
mod writer;

pub use buf::{multi_ring_buf, ring_buf};
pub use reader::MultiRingReader;
pub use reader::Reader;
pub use reader::RingReader;
pub use writer::MultiRingWriter;
pub use writer::RingWriter;
pub use writer::Writer;
