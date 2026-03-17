use std::{cell::Cell, sync::Arc};

use crate::buf::{MultiRingBuf, RingBuf};

/// Shared functionality of ring buffer readers
pub trait Reader {
    /// Gets the current (real) read position within the ring buffer.
    fn read_position(&self) -> usize;
    /**
    Gets the read capacity of the ring buffer, the number of values from the
    read position the ring buffer will save from being overwritten.
    */
    fn read_capacity(&self) -> usize;
    /**
    Retrieves the real write position from the associated ring buffer using an
    atomic read operation, caching the result within this reader.
     */
    fn real_write_position(&self) -> usize;
    /**
    Returns the cached write position from this reader, avoiding an atomic
    read operation.
     */
    fn cached_write_position(&self) -> usize;
    /**
    Moves the read position forward by the given `amount`.
     */
    fn advance_read_position_by(&self, amount: usize);
    /**
    Moves the read position forward by 1.
     */
    #[inline]
    fn advance_read_position(&self) {
        self.advance_read_position_by(1);
    }
    /**
    Calculates the real amount of values available for reading from the ring
    buffer, caching the retrieved write position within this reader.
     */
    #[inline]
    fn real_reads_available(&self) -> usize {
        let real_write_position = self.real_write_position();
        reads_available(self, real_write_position)
    }
    /**
    Calculates the amount of values available for reading from the ring buffer
    based on what the real write position was the last time it was retrieved
    and cached.

    This is useful if you would like to avoid an atomic read operation.
    */
    #[inline]
    fn cached_reads_available(&self) -> usize {
        let cached_write_position = self.cached_write_position();
        reads_available(self, cached_write_position)
    }
    /**
    Returns true if at least one value is available for reading from the ring
    buffer.
    */
    #[inline]
    fn read_is_available(&self) -> bool {
        if could_be_empty(self) {
            return !is_really_empty(self);
        }
        true
    }
    /**
    Returns true if the given `amount` of values are available for reading from
    the ring buffer.
    */
    #[inline]
    fn reads_are_available(&self, amount: usize) -> bool {
        if self.cached_reads_available() < amount {
            return self.real_reads_available() >= amount;
        }
        true
    }
}

#[inline]
fn reads_available<T>(reader: &T, write_position: usize) -> usize
where
    T: ?Sized + Reader,
{
    let read_position = reader.read_position();
    write_position - read_position
}
#[inline]
fn index_is_available<T>(reader: &T, index: usize) -> bool
where
    T: ?Sized + Reader,
{
    if index < reader.cached_reads_available() {
        return true;
    }
    if index < reader.real_reads_available() {
        return true;
    }
    false
}
#[inline]
fn index_is_available_signed<T>(reader: &T, index: isize) -> bool
where
    T: ?Sized + Reader,
{
    if index <= -reader.read_capacity().cast_signed() {
        return false;
    }
    if index < reader.cached_reads_available().cast_signed() {
        return true;
    }
    if index < reader.real_reads_available().cast_signed() {
        return true;
    }
    false
}
#[inline]
fn could_be_empty<T: ?Sized + Reader>(reader: &T) -> bool {
    let read_position = reader.read_position();
    let cached_write_position = reader.cached_write_position();
    empty(read_position, cached_write_position)
}
#[inline]
fn is_really_empty<T: ?Sized + Reader>(reader: &T) -> bool {
    let read_position = reader.read_position();
    let real_write_position = reader.real_write_position();
    empty(read_position, real_write_position)
}
#[inline]
fn empty(read_position: usize, write_position: usize) -> bool {
    read_position >= write_position
}

/// Reader for a single-channel buffer
#[derive(Debug)]
pub struct RingReader<T> {
    pub(crate) buffer: Arc<RingBuf<T>>,
    cached_read_pos: Cell<usize>,
    cached_write_pos: Cell<usize>,
}
impl<T: Copy + Default> Reader for RingReader<T> {
    #[inline]
    fn read_position(&self) -> usize {
        self.cached_read_pos.get()
    }
    #[inline]
    fn read_capacity(&self) -> usize {
        self.buffer.read_capacity()
    }
    #[inline]
    fn real_write_position(&self) -> usize {
        let write_position = self.buffer.write_position();
        self.cached_write_pos.set(write_position);
        write_position
    }
    #[inline]
    fn cached_write_position(&self) -> usize {
        self.cached_write_pos.get()
    }
    #[inline]
    fn advance_read_position_by(&self, amount: usize) {
        let new = self.buffer.advance_read_position_by(amount);
        self.cached_read_pos.set(new);
    }
}
impl<'a, T: Default + Copy> RingReader<T> {
    #[inline]
    pub(crate) fn new(buffer: Arc<RingBuf<T>>) -> Self {
        Self {
            buffer,
            cached_read_pos: Default::default(),
            cached_write_pos: Default::default(),
        }
    }
    /**
    Reads the value at the current read position.
    */
    #[inline]
    pub fn read(&self) -> T {
        let read_position = self.read_position();
        self.read_at_index(read_position)
    }
    /**
    Reads a value at or ahead of the current read position.
    `index` is relative to the current read position.

    # Panics
    This function will panic when `index >= self.real_reads_available()`.
    */
    #[inline]
    pub fn read_forward(&self, index: usize) -> T {
        assert!(index_is_available(self, index));
        self.read_forward_unchecked(index)
    }
    /**
    Reads a value at or ahead of the current read position without checking if
    the index is valid.
    `index` is relative to the current read position.

    Passing an invalid index is memory-safe, but will likely return a value
    from an undesired position within the buffer.
    */
    #[inline]
    pub fn read_forward_unchecked(&self, index: usize) -> T {
        let index = self.read_position() + index;
        self.read_at_index(index)
    }
    /**
    Reads a value at or behind the current read position.
    `index` is relative to the current read position.

    # Panics
    This function will panic when `index >= self.read_capacity()`.
    */
    #[inline]
    pub fn read_backward(&self, index: usize) -> T {
        let valid = (index < self.read_capacity()) & self.read_is_available();
        assert!(valid);
        self.read_backward_unchecked(index)
    }
    /**
    Reads a value at or behind the current read position without checking if
    the index is valid.

    `index` is relative to the current read position.
    Passing an invalid index is memory-safe, but will likely return a value
    from an undesired position within the buffer.
    */
    #[inline]
    pub fn read_backward_unchecked(&self, index: usize) -> T {
        let index = self.read_position().saturating_sub(index);
        self.read_at_index(index)
    }
    #[inline]
    /**
    Reads a value with an signed index relative to the current read position.

    A negative index reads backward and a positive index reads forward.

    # Panics
    This function will panic when `index <= -self.read_capacity()` or
    `index >= self.real_reads_available()`.
    */
    pub fn read_relative(&self, index: isize) -> T {
        assert!(self.read_is_available());
        assert!(index_is_available_signed(self, index));
        self.read_relative_unchecked(index)
    }
    /**
    Reads a value with an signed index relative to the current read position
    without checking if the index is valid.

    A negative index reads backward and a positive index reads forward.

    Passing an invalid index is memory-safe, but will likely return a value
    from an undesired position within the buffer.
    */
    #[inline]
    pub fn read_relative_unchecked(&self, index: isize) -> T {
        let index = self.read_position().saturating_add_signed(index);
        self.read_at_index(index)
    }
    #[inline]
    fn read_at_index(&self, index: usize) -> T {
        self.buffer.read_at_index(index)
    }
    /**
    Returns true if the associated writer has been dropped, meaning
    no further writes will occur in the ring buffer.
    */
    #[inline]
    pub fn is_closed(&self) -> bool {
        Arc::strong_count(&self.buffer) < 2
    }
}
unsafe impl<'a, T> Send for RingReader<T> {}

/// Reader for a multi-channel buffer
#[derive(Debug)]
pub struct MultiRingReader<T> {
    pub(crate) buffer: Arc<MultiRingBuf<T>>,
    cached_read_pos: Cell<usize>,
    cached_write_pos: Cell<usize>,
}
impl<T: Copy + Default> Reader for MultiRingReader<T> {
    #[inline]
    fn read_position(&self) -> usize {
        self.cached_read_pos.get()
    }
    #[inline]
    fn read_capacity(&self) -> usize {
        self.buffer.read_capacity()
    }
    #[inline]
    fn real_write_position(&self) -> usize {
        let write_position = self.buffer.write_position();
        self.cached_write_pos.set(write_position);
        write_position
    }
    #[inline]
    fn cached_write_position(&self) -> usize {
        self.cached_write_pos.get()
    }
    #[inline]
    fn advance_read_position_by(&self, amount: usize) {
        let new = self.buffer.advance_read_position_by(amount);
        self.cached_read_pos.set(new);
    }
}
impl<'a, T: Default + Copy> MultiRingReader<T> {
    #[inline]
    pub(crate) fn new(buffer: Arc<MultiRingBuf<T>>) -> Self {
        Self {
            buffer,
            cached_read_pos: Default::default(),
            cached_write_pos: Default::default(),
        }
    }
    /**
    Reads the value at the current read position.
    */
    #[inline]
    pub fn read(&self, channel: usize) -> T {
        let read_position = self.read_position();
        self.read_at_index(channel, read_position)
    }
    /**
    Reads a value at or ahead of the current read position.

    `index` is relative to the current read position.

    # Panics
    This function will panic when `index >= self.real_reads_available()`.
    */
    #[inline]
    pub fn read_forward(&self, channel: usize, index: usize) -> T {
        assert!(index_is_available(self, index));
        self.read_forward_unchecked(channel, index)
    }
    /**
    Reads a value at or ahead of the current read position without checking if
    the index is valid.

    `index` is relative to the current read position.

    Passing an invalid index is memory-safe, but will likely return a value
    from an undesired position within the buffer.
    */
    #[inline]
    pub fn read_forward_unchecked(&self, channel: usize, index: usize) -> T {
        let index = self.read_position() + index;
        self.read_at_index(channel, index)
    }
    /**
    Reads a value at or behind the current read position.

    `index` is relative to the current read position.

    # Panics
    This function will panic when `index >= self.read_capacity()`.
    */
    #[inline]
    pub fn read_backward(&self, channel: usize, index: usize) -> T {
        assert!(index < self.read_capacity());
        self.read_backward_unchecked(channel, index)
    }
    /**
    Reads a value at or behind the current read position without checking if
    the index is valid.

    `index` is relative to the current read position.

    Passing an invalid index is memory-safe, but will likely return a value
    from an undesired position within the buffer.
    */
    #[inline]
    pub fn read_backward_unchecked(&self, channel: usize, index: usize) -> T {
        let index = self.read_position().saturating_sub(index);
        self.read_at_index(channel, index)
    }
    /**
    Reads a value with an signed index relative to the current read position.

    A negative index reads backward and a positive index reads forward.

    # Panics
    This function will panic when `index <= -self.read_capacity()` or
    `index >= self.real_reads_available()`.
    */
    #[inline]
    pub fn read_relative(&self, channel: usize, index: isize) -> T {
        assert!(index_is_available_signed(self, index));
        self.read_relative_unchecked(channel, index)
    }
    /**
    Reads a value with an signed index relative to the current read position
    without checking if the index is valid.

    A negative index reads backward and a positive index reads forward.

    Passing an invalid index is memory-safe, but will likely return a value
    from an undesired position within the buffer.
    */
    #[inline]
    pub fn read_relative_unchecked(&self, channel: usize, index: isize) -> T {
        let index = self.read_position().saturating_add_signed(index);
        self.read_at_index(channel, index)
    }
    #[inline]
    fn read_at_index(&self, channel: usize, index: usize) -> T {
        self.buffer.read_at_index(channel, index)
    }
    /**
    Returns true if the associated writer has been dropped, meaning
    no further writes will occur in the ring buffer.
    */
    #[inline]
    pub fn is_closed(&self) -> bool {
        Arc::strong_count(&self.buffer) < 2
    }
    #[inline]
    fn read_capacity(&self) -> usize {
        self.buffer.read_capacity()
    }
    /// Returns the number of channels in this multi-channel ring buffer.
    #[inline]
    pub fn channels(&self) -> usize {
        self.buffer.channels()
    }
}
unsafe impl<'a, T> Send for MultiRingReader<T> {}
