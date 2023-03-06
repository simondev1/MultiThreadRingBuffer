using System;
using System.Diagnostics;
using System.Threading;

namespace AdditionalGenerics
{
	/// <summary>
	/// This is a ring buffer or queue.
	/// Intended for multiple threads to enqueue/set items at any index using the methods SetAt() or TrySetAt().
	/// SetAt() and TrySetAt() are multi thread safe.
	/// However Dequeue() is NOT multi thread safe and must be used by only one thread!
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <remarks>
	/// Think of this class as a queue, except that multiple threads are allowed to enqueue, and enqueue is not needed to be in sequence.
	/// The item at a certain index can only be set once.
	/// 
	/// Multi thread safety:
	///  - Dequeue() is not multi thread safe and must be used by only one thread.
	///    Multiple dequeue threads will result in non-deterministic behavier.
	///	 - Enqueueing is multi thread safe: It is safe for additional multiple threads to simultaneously call SetAt() and/or TrySetAt().
	///	 - All other members (FirstRingBufferIndex, LastRingBufferIndex, IsItemReady, IsSet) are not multi thread safe, however the impact is weak:
	///	   Multiple threads using these other members might result in exceptions that must be handled however the queue will always be correct and deterministic.
	/// 
	/// Locking internals:
	///  - Dequeue() has an internal writelock because it changes an internal index.
	///  - SetAt() and TrySetAt() do only read this internal index, therefore they have an internal readlock.
	///  - Other members do not have an internal lock.
	///  
	/// Design of enqueue threads:
	///  - If all the following is known for sure, then locking is not needed at all:
	///    a) Index of the item.
	///    b) The index is between or among FirstRingBufferIndex and LastRingBufferIndex.
	///    c) A certain index will only be set once.
	///  - If a) and c) are known, and b) is likely but not sure, then it is recommended to simply call SetAt() or TrySetAt() and handle the failes.
	///  - If more of this information is unkown, but can be caluclated by getting information from the MultiThreadRingBuffer:
	///    A readLock might be a good approach: See summary of GetIndexReaderWriterLockSlim()
	///  
	/// </remarks>
	[Serializable]
	[DebuggerDisplay("FirstRingBufferIndex = {FirstRingBufferIndex}, LastRingBufferIndex = {LastRingBufferIndex}")]
	public class MultiThreadRingBuffer<T>
	{
		private const string exceptionItemNotSet = "Item at index '{0}' has not been set and therefore cannot be dequeued.";
		private const string exceptionIndexOutOfRange = "Index was outside the bounds of the array..";
		private const string exceptionItemAlreadySet = "Cannot set item at index '{0}' again. Item at this index was already set.";

		private ulong _index;
		private readonly ulong _length;
		private readonly T[] _buffer;
		private readonly bool[] _isSet;
		private readonly ReaderWriterLockSlim indexLock;


		/// <summary>
		/// Calculate index modulo length.
		/// Check for length == 0.
		/// </summary>
		/// <param name="index">index</param>
		/// <returns>index modulo Length</returns>
		/// <exception cref="System.IndexOutOfRangeException">When Length is zero.</exception>
		private ulong indexModLength(ulong index)
		{
			// Prevent DivideByZeroException and throw the correct IndexOutOfRangeException.
			if (0 == Length)
			{
				throw new IndexOutOfRangeException(exceptionIndexOutOfRange);
			}

			return index % Length;
		}

		/// <summary>
		/// Set an item at indexModulo without checks.
		/// </summary>
		/// <param name="indexModulo">Index must already be modulo Length.</param>
		/// <param name="item">The item to set.</param>
		private void SetAtInternal(ulong indexModulo, T item)
		{
			_buffer[indexModulo] = item;
			_isSet[indexModulo] = true;
		}


		/// <summary>
		/// Allocates the buffer.
		/// Requires space for two arrays: T[length] and bool[length].
		/// </summary>
		/// <param name="length">The number of items the queue can contain.</param>
		public MultiThreadRingBuffer(ulong length, ulong continuationIndex)
		{
			_index = continuationIndex;
			_length = length;
			_buffer = new T[_length];
			_isSet = new bool[_length];
			indexLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
		}

		/// <summary>
		/// Dequeues an item.
		/// Not multi thread safe. (Only one thread should dequeue items.)
		/// </summary>
		/// <returns>The dequeued item.</returns>
		/// <exception cref="System.InvalidOperationException">When the item at the current index is not yet set.</exception>
		/// <exception cref="System.OverflowException">When dequeueing the 18,446,744,073,709,551,615'th item.</exception>
		/// <exception cref="System.IndexOutOfRangeException">When Length is zero.</exception>
		public virtual T Dequeue()
		{
			ulong indexModulo = indexModLength(_index);

			if (!_isSet[indexModulo])
			{
				throw new InvalidOperationException(string.Format(exceptionItemNotSet, _index));
			}

			T result = _buffer[indexModulo];

			try
			{
				indexLock.EnterWriteLock();

				_isSet[indexModulo] = false;
				checked
				{
					_index++;
				}
			}
			finally
			{
				indexLock.ExitWriteLock();
			}

			return result;
		}
		
		/// <summary>
		/// Set or 'enqueue' an item at a certain index.
		/// The item at a certain index can only be set once.
		/// </summary>
		/// <param name="index">The index of the item.</param>
		/// <param name="item">The item to set.</param>
		/// <exception cref="System.InvalidOperationException">When an item at this index was already set.</exception>
		/// <exception cref="System.IndexOutOfRangeException">When index is not the range FirstRingBufferIndex to LastRingBufferIndex. Or when Length is zero.</exception>
		public virtual void SetAt(ulong index, T item)
		{
			ulong indexModulo = indexModLength(index);

			try
			{
				indexLock.EnterReadLock();

				if (index > LastRingBufferIndex || index < FirstRingBufferIndex)
				{
					throw new IndexOutOfRangeException(exceptionIndexOutOfRange);
				}

				if (_isSet[indexModulo])
				{
					throw new InvalidOperationException(string.Format(exceptionItemAlreadySet, index));
				}
				SetAtInternal(indexModulo, item);
			}
			finally
			{
				indexLock.ExitReadLock();
			}

		}

		/// <summary>
		/// Set or 'enqueue' an item at a certain index.
		/// The item at a certain index can only be set once.
		/// </summary>
		/// <param name="index">The index of the item.</param>
		/// <param name="item">The item to set.</param>
		/// <returns>true = success, false = index is out of range or item at this index was already set or Length is zero.</returns>
		public virtual bool TrySetAt(ulong index, T item)
		{
			if (0 == Length) return false;

			ulong indexModulo = index % Length;

			try
			{
				indexLock.EnterReadLock();

				if (index > LastRingBufferIndex || index < FirstRingBufferIndex) return false;
				if (_isSet[indexModulo]) return false;

				SetAtInternal(indexModulo, item);
			}
			finally
			{
				indexLock.ExitReadLock();
			}
			return true;
		}

		/// <summary>
		/// Get the smallest index of the ring buffer.
		/// The next call on Dequeue() will try to dequeue this index.
		/// </summary>
		public virtual ulong FirstRingBufferIndex
		{
			get
			{
				return _index;
			}
		}

		/// <summary>
		/// Gets the highest index of the ring buffer.
		/// </summary>
		public virtual ulong LastRingBufferIndex
		{
			get
			{
				try
				{
					checked
					{
						return (_index + _length - 1);
					}
				}
				catch (OverflowException)
				{
					return ulong.MaxValue;
				}
			}
		}

		/// <summary>
		/// True when an item is ready to be dequeued. Otherwise false.
		/// </summary>
		/// <exception cref="System.IndexOutOfRangeException">When Length is zero.</exception>
		public virtual bool IsItemReady
		{
			get
			{
				return _isSet[indexModLength(_index)];
			}
		}

		/// <summary>
		/// True when an item was set at the index. Otherwise false.
		/// Also true for indexes that have already been dequeued.
		/// Also false for indexes that cannot yet be set.
		/// </summary>
		/// <param name="index">The index.</param>
		/// <exception cref="System.IndexOutOfRangeException">When Length is zero.</exception>
		public virtual bool IsSet(ulong index)
		{
			if (index < FirstRingBufferIndex) return true;
			if (index > LastRingBufferIndex) return false;

			return _isSet[indexModLength(index)];
		}

		/// <summary>
		/// Gets the index lock object.
		/// Allows to calculate the index or item depending on IsSet or FirstRingBufferIndex or LastRingBufferIndex.
		/// It is recommended to not use this property and instead have a concept where item and corresponding index can be calulated independent of IsSet or FirstRingBufferIndex or LastRingBufferIndex.
		/// Dequeue does writeLock when internal index is incremented. SetAt() and TrySetAt() already have a readLock.
		/// The lock policy is LockRecursionPolicy.SupportsRecursion.
		/// 
		/// Example pseudo code:
		/// 
		///		try
		///		{
		///			// lock such that the index won't change
		///			multiThreadRingBuffer.GetIndexReaderWriterLockSlim.EnterReadLock();
		///			
		///			// calculate an index or item depending on IsSet or FirstRingBufferIndex or LastRingBufferIndex
		///			...
		///			
		///			// enqueue the item
		///			multiThreadRingBuffer.SetAt(index, item);
		///		}
		///		finally
		///		{
		///			multiThreadRingBuffer.GetIndexReaderWriterLockSlim.ExitReadLock();
		///		}
		///  
		/// </summary>
		public ReaderWriterLockSlim GetIndexReaderWriterLockSlim
		{
			get
			{
				return indexLock;
			}
		}

		/// <summary>
		/// Gets the length of the MultiThreadRingBuffer.
		/// </summary>
		public ulong Length
		{
			get
			{
				return _length;
			}
		}

	}

}
