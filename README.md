# wreath
A SPSC ring buffer library with multi-channel
buffers and capabilities to retain readable data.

![Crates.io Version](https://img.shields.io/crates/v/wreath)
![docs.rs](https://img.shields.io/docsrs/wreath)

## Development Status
wreath is early in development.
There is still testing to be done.
Breaking API changes are to be expected.

## Features
wreath has a few special features that might make it a better fit for your
project when compared to other libraries.

### Retaining data
Buffers can reserve a given length of items behind the read position.
This length is called the 'read capacity'.

### Multichannel buffers
Multichannel buffers can be thought of as multiple ring buffers that share
a single read and write position, as well as read and write capacity.

### Capacity expansion
wreath allocates power-of-two lengths for its buffers to avoid
potentially expensive remainder operations during indexing.

Upon creating a buffer, wreath will allocate the smallest power-of-two length
greater than the total required capacity and use any extra space for additional
write capacity.

## Safety
All data that wreath allocates within its ring buffers is initialized
and indexes always wrap around the buffer. Incorrectly indexing or advancing
read/write positions may produce unexpected results, but should not produce
undefined behavior. Therefore, wreath allows for unchecked indexing and
advancing of positions without the need for `unsafe` blocks.

## Limitations
Currently, total allocated lengths of buffers are limited to power-of-two
lengths. In the future this library may allow the user to create buffers that
use other lengths.
