use std::{cell::Cell, sync::Arc};

use crate::buf::{MultiRingBuf, RingBuf};

/// Shared functionality of ring buffer writers
pub trait Writer {
    /// Gets the current (real) write position within the ring buffer.
    fn write_position(&self) -> usize;
    /**
    Gets the write capacity of the ring buffer, the number of values the ring
    buffer can write ahead of its read position.
    */
    fn write_capacity(&self) -> usize;
    /**
    Retrieves the real read position from the associated ring buffer using an
    atomic read operation, caching the result within this reader.
     */
    fn real_read_position(&self) -> usize;
    /**
    Returns the cached read position from this reader, avoiding an atomic
    read operation.
     */
    fn cached_read_position(&self) -> usize;
    /**
    Moves the write position forward by the given `amount`.
     */
    fn advance_write_position_by(&self, amount: usize);
    /**
    Moves the write position forward by 1.
     */
    #[inline]
    fn advance_write_position(&self) {
        self.advance_write_position_by(1);
    }
    /**
    Calculates the real amount of values available for writing to the ring
    buffer, caching the retrieved read position within this writer.
     */
    #[inline]
    fn real_writes_available(&self) -> usize {
        let real_read_position = self.real_read_position();
        writes_available(self, real_read_position)
    }
    /**
    Calculates the amount of values available for writing to the ring buffer
    based on what the real read position was the last time it was retrieved and
    cached.

    This is useful if you would like to avoid an atomic read operation.
    */
    #[inline]
    fn cached_writes_available(&self) -> usize {
        let cached_read_position = self.cached_read_position();
        writes_available(self, cached_read_position)
    }
    /**
    Returns true if at least one value is available for writing to the ring
    buffer.
    */
    #[inline]
    fn write_is_available(&self) -> bool {
        if could_be_full(self) {
            return !is_really_full(self);
        }
        true
    }
    /**
    Returns true if the given `amount` of values are available for writing to
    the ring buffer.
    */
    #[inline]
    fn writes_are_available(&self, amount: usize) -> bool {
        if self.cached_writes_available() < amount {
            return self.real_writes_available() >= amount;
        }
        true
    }
}
#[inline]
fn writes_available<T>(writer: &T, read_position: usize) -> usize
where
    T: ?Sized + Writer,
{
    let write_position = writer.write_position();
    let write_capacity = writer.write_capacity();
    write_capacity - write_position.saturating_sub(read_position)
}
#[inline]
fn index_is_available<T>(writer: &T, index: usize) -> bool
where
    T: ?Sized + Writer,
{
    if index < writer.cached_writes_available() {
        return true;
    }
    if index < writer.real_writes_available() {
        return true;
    }
    false
}
#[inline]
fn could_be_full<T: ?Sized + Writer>(writer: &T) -> bool {
    let write_position = writer.write_position();
    let cached_read_position = writer.cached_read_position();
    let write_capacity = writer.write_capacity();
    full(cached_read_position, write_position, write_capacity)
}
#[inline]
fn is_really_full<T: ?Sized + Writer>(writer: &T) -> bool {
    let write_position = writer.write_position();
    let real_read_position = writer.real_read_position();
    let write_capacity = writer.write_capacity();
    full(real_read_position, write_position, write_capacity)
}
#[inline]
fn full(read_position: usize, write_position: usize, write_capacity: usize) -> bool {
    write_position.saturating_sub(read_position) >= write_capacity
}

/// Writer for a single-channel buffer
#[derive(Debug)]
pub struct RingWriter<T> {
    pub(crate) buffer: Arc<RingBuf<T>>,
    cached_read_pos: Cell<usize>,
    cached_write_pos: Cell<usize>,
}
impl<T: Copy + Default> Writer for RingWriter<T> {
    #[inline]
    fn write_position(&self) -> usize {
        self.cached_write_pos.get()
    }
    #[inline]
    fn write_capacity(&self) -> usize {
        self.buffer.write_capacity()
    }
    #[inline]
    fn real_read_position(&self) -> usize {
        let read_position = self.buffer.read_position();
        self.cached_read_pos.set(read_position);
        read_position
    }
    #[inline]
    fn cached_read_position(&self) -> usize {
        self.cached_read_pos.get()
    }
    #[inline]
    fn advance_write_position_by(&self, amount: usize) {
        let new = self.buffer.advance_write_position_by(amount);
        self.cached_write_pos.set(new);
    }
}

impl<'a, T: Default + Copy> RingWriter<T> {
    #[inline]
    pub(crate) fn new(buffer: Arc<RingBuf<T>>) -> Self {
        Self {
            buffer,
            cached_read_pos: Default::default(),
            cached_write_pos: Default::default(),
        }
    }
    /**
    Writes a value to the current write position.
    */
    #[inline]
    pub fn write(&self, value: T) {
        let write_position = self.write_position();
        self.write_at_index(write_position, value);
    }
    /**
    Writes a value at or ahead of the current write position.

    `index` is relative to the current write position.

    # Panics
    This function will panic when `index >= self.real_writes_available()`.
    */
    #[inline]
    pub fn write_forward(&self, index: usize, value: T) {
        assert!(index_is_available(self, index));
        self.write_forward_unchecked(index, value);
    }
    /**
    Writes a value at or ahead of the current write position without checking if
    the index is valid.

    `index` is relative to the current write position.

    Passing an invalid index is memory-safe, but will likely write the
    value to an undesired position within the buffer.
    */
    #[inline]
    pub fn write_forward_unchecked(&self, index: usize, value: T) {
        let index = self.cached_write_pos.get() + index;
        self.write_at_index(index, value);
    }
    #[inline]
    fn write_at_index(&self, index: usize, value: T) {
        self.buffer.write_at_index(index, value);
    }
    /**
    Returns a mutable reference to the value at the current write position.
    */
    #[inline]
    pub fn mutate(&mut self) -> &mut T {
        let index = self.write_position();
        self.mutate_at_index(index)
    }
    /**
    Returns a mutable reference to a value at or ahead of the current write
    position.

    `index` is relative to the current write position.

    # Panics
    This function will panic when `index >= self.real_writes_available()`.
    */
    #[inline]
    pub fn mutate_forward(&mut self, index: usize) -> &mut T {
        assert!(index_is_available(self, index));
        self.mutate_forward_unchecked(index)
    }
    /**
    Returns a mutable reference to a value at or ahead of the current write
    position without checking if the index is valid.

    `index` is relative to the current write position.

    Passing an invalid index is memory-safe, but will likely mutate a
    value at an undesired position within the buffer.
    */
    #[inline]
    pub fn mutate_forward_unchecked(&mut self, index: usize) -> &mut T {
        let index = self.write_position() + index;
        self.mutate_at_index(index)
    }
    #[inline]
    fn mutate_at_index(&mut self, index: usize) -> &mut T {
        self.buffer.mutate_at_index(index)
    }
    #[inline]
    /// Returns true if the associated reader has been dropped, meaning
    /// no further reads will occur in the ring buffer.
    pub fn is_closed(&self) -> bool {
        Arc::strong_count(&self.buffer) < 2
    }
}
unsafe impl<'a, T> Send for RingWriter<T> {}

/// Writer for a multi-channel buffer
#[derive(Debug)]
pub struct MultiRingWriter<T> {
    pub(crate) buffer: Arc<MultiRingBuf<T>>,
    cached_read_pos: Cell<usize>,
    cached_write_pos: Cell<usize>,
}
impl<T: Copy + Default> Writer for MultiRingWriter<T> {
    #[inline]
    fn write_position(&self) -> usize {
        self.cached_write_pos.get()
    }
    #[inline]
    fn write_capacity(&self) -> usize {
        self.buffer.write_capacity()
    }
    #[inline]
    fn real_read_position(&self) -> usize {
        let read_position = self.buffer.read_position();
        self.cached_read_pos.set(read_position);
        read_position
    }
    #[inline]
    fn cached_read_position(&self) -> usize {
        self.cached_read_pos.get()
    }
    #[inline]
    fn advance_write_position_by(&self, amount: usize) {
        let new = self.buffer.advance_write_position_by(amount);
        self.cached_write_pos.set(new);
    }
}
impl<'a, T: Default + Copy> MultiRingWriter<T> {
    #[inline]
    pub(crate) fn new(buffer: Arc<MultiRingBuf<T>>) -> Self {
        Self {
            buffer,
            cached_read_pos: Default::default(),
            cached_write_pos: Default::default(),
        }
    }
    /**
    Writes a value to the current write position.
    */
    #[inline]
    pub fn write(&self, channel: usize, value: T) {
        let write_position = self.write_position();
        self.write_at_index(channel, write_position, value);
    }
    /**
    Writes a value at or ahead of the current write position.

    `index` is relative to the current write position.

    # Panics
    This function will panic when `index >= self.real_writes_available()`.
    */
    #[inline]
    pub fn write_forward(&self, channel: usize, index: usize, value: T) {
        assert!(index_is_available(self, index));
        self.write_forward_unchecked(channel, index, value);
    }
    /**
    Writes a value at or ahead of the current write position without checking if
    the index is valid.

    `index` is relative to the current write position.

    Passing an invalid index is memory-safe, but will likely write the
    value to an undesired position within the buffer.
    */
    #[inline]
    pub fn write_forward_unchecked(&self, channel: usize, index: usize, value: T) {
        let index = self.write_position() + index;
        self.write_at_index(channel, index, value);
    }
    #[inline]
    fn write_at_index(&self, channel: usize, index: usize, value: T) {
        self.buffer.write_at_index(channel, index, value);
    }
    /**
    Returns a mutable reference to the value at the current write position.
    */
    #[inline]
    pub fn mutate(&mut self, channel: usize) -> &mut T {
        let index = self.write_position();
        self.mutate_at_index(channel, index)
    }
    /**
    Returns a mutable reference to a value at or ahead of the current write
    position.

    `index` is relative to the current write position.

    # Panics
    This function will panic when `index >= self.real_writes_available()`.
    */
    #[inline]
    pub fn mutate_forward(&mut self, channel: usize, index: usize) -> &mut T {
        assert!(index_is_available(self, index));
        self.mutate_forward_unchecked(channel, index)
    }
    /**
    Returns a mutable reference to a value at or ahead of the current write
    position without checking if the index is valid.

    `index` is relative to the current write position.

    Passing an invalid index is memory-safe, but will likely mutate a
    value at an undesired position within the buffer.
    */
    #[inline]
    pub fn mutate_forward_unchecked(&mut self, channel: usize, index: usize) -> &mut T {
        let index = self.write_position() + index;
        self.mutate_at_index(channel, index)
    }
    #[inline]
    fn mutate_at_index(&mut self, channel: usize, index: usize) -> &mut T {
        self.buffer.mutate_at_index(channel, index)
    }
    /// Returns true if the associated reader has been dropped, meaning
    /// no further reads will occur in the ring buffer.
    #[inline]
    pub fn is_closed(&self) -> bool {
        Arc::strong_count(&self.buffer) < 2
    }
    /// Returns the number of channels in this multi-channel ring buffer.
    #[inline]
    pub fn channels(&self) -> usize {
        self.buffer.channels()
    }
}
unsafe impl<'a, T> Send for MultiRingWriter<T> {}
