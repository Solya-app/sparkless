"""Tests for processing lock functionality."""

import time

import pytest

from sparkless import SparkSession


@pytest.fixture
def memory_spark():
    """Create a SparkSession with memory backend for lock tests."""
    session = SparkSession("test_locks", backend_type="memory")
    yield session
    session.stop()


class TestProcessingLocks:
    """Tests for lock management in MemoryStorageManager."""

    def test_acquire_lock(self, memory_spark):
        """Test acquiring a lock."""
        result = memory_spark._storage.acquire_lock("task_lock_1", "worker_1")
        assert result is True

        info = memory_spark._storage.get_lock_info("task_lock_1")
        assert info is not None
        assert info["owner"] == "worker_1"

    def test_acquire_lock_already_held(self, memory_spark):
        """Test acquiring a lock already held by another owner."""
        memory_spark._storage.acquire_lock("task_lock_2", "worker_1")

        # Another worker tries to acquire
        result = memory_spark._storage.acquire_lock("task_lock_2", "worker_2")
        assert result is False

    def test_acquire_lock_same_owner(self, memory_spark):
        """Test re-acquiring a lock by the same owner."""
        memory_spark._storage.acquire_lock("task_lock_3", "worker_1")

        # Same owner can re-acquire
        result = memory_spark._storage.acquire_lock("task_lock_3", "worker_1")
        assert result is True

    def test_release_lock(self, memory_spark):
        """Test releasing a lock."""
        memory_spark._storage.acquire_lock("task_lock_4", "worker_1")

        result = memory_spark._storage.release_lock("task_lock_4", "worker_1")
        assert result is True
        assert memory_spark._storage.get_lock_info("task_lock_4") is None

    def test_release_lock_wrong_owner(self, memory_spark):
        """Test releasing a lock with wrong owner."""
        memory_spark._storage.acquire_lock("task_lock_5", "worker_1")

        # Wrong owner tries to release
        result = memory_spark._storage.release_lock("task_lock_5", "worker_2")
        assert result is False

        # Lock still held
        assert memory_spark._storage.is_lock_held("task_lock_5") is True

    def test_release_nonexistent_lock(self, memory_spark):
        """Test releasing a lock that does not exist."""
        result = memory_spark._storage.release_lock("nonexistent_lock", "worker_1")
        assert result is False

    def test_lock_with_metadata(self, memory_spark):
        """Test lock with metadata."""
        memory_spark._storage.acquire_lock(
            "task_lock_6",
            "worker_1",
            metadata={"org_id": "org-123", "task_name": "GOLD_KPI"},
        )

        info = memory_spark._storage.get_lock_info("task_lock_6")
        assert info is not None
        assert info["metadata"]["org_id"] == "org-123"
        assert info["metadata"]["task_name"] == "GOLD_KPI"

    def test_lock_timeout(self, memory_spark):
        """Test lock expiration."""
        # Acquire lock with very short timeout
        memory_spark._storage.acquire_lock("task_lock_7", "worker_1", timeout_seconds=1)

        assert memory_spark._storage.is_lock_held("task_lock_7") is True

        # Wait for expiration
        time.sleep(1.1)

        assert memory_spark._storage.is_lock_held("task_lock_7") is False

        # Another worker can now acquire
        result = memory_spark._storage.acquire_lock("task_lock_7", "worker_2")
        assert result is True

    def test_list_locks(self, memory_spark):
        """Test listing all locks."""
        memory_spark._storage.reset_locks()
        memory_spark._storage.acquire_lock("lock_a", "worker_1")
        memory_spark._storage.acquire_lock("lock_b", "worker_2")

        locks = memory_spark._storage.list_locks()
        assert len(locks) == 2
        assert "lock_a" in locks
        assert "lock_b" in locks

    def test_cleanup_expired_locks(self, memory_spark):
        """Test cleanup of expired locks."""
        memory_spark._storage.reset_locks()
        memory_spark._storage.acquire_lock("lock_short", "worker_1", timeout_seconds=1)
        memory_spark._storage.acquire_lock(
            "lock_long", "worker_2", timeout_seconds=3600
        )

        time.sleep(1.1)

        removed = memory_spark._storage.cleanup_expired_locks()
        assert removed == 1

        locks = memory_spark._storage.list_locks()
        assert "lock_short" not in locks
        assert "lock_long" in locks

    def test_reset_locks(self, memory_spark):
        """Test resetting all locks."""
        memory_spark._storage.acquire_lock("lock_x", "worker_1")
        memory_spark._storage.reset_locks()

        assert len(memory_spark._storage.list_locks()) == 0

    def test_get_lock_info_nonexistent(self, memory_spark):
        """Test getting info for a lock that does not exist."""
        info = memory_spark._storage.get_lock_info("nonexistent_lock")
        assert info is None

    def test_is_lock_held_nonexistent(self, memory_spark):
        """Test checking if a nonexistent lock is held."""
        result = memory_spark._storage.is_lock_held("nonexistent_lock")
        assert result is False

    def test_lock_default_timeout(self, memory_spark):
        """Test lock with default timeout (1 hour)."""
        memory_spark._storage.acquire_lock("task_lock_default", "worker_1")

        info = memory_spark._storage.get_lock_info("task_lock_default")
        assert info is not None
        assert info["timeout_seconds"] == 3600

    def test_lock_via_sql_insert(self, memory_spark):
        """Test lock management via SQL INSERT and SELECT."""
        # Create locks table
        memory_spark.sql(
            """CREATE TABLE test_db.task_locks (
                lock_id STRING,
                owner STRING
            )"""
        )

        # Acquire lock via INSERT
        memory_spark.sql(
            """INSERT INTO test_db.task_locks
               VALUES ('lock_1', 'worker_1')"""
        )

        # Verify lock exists
        result = memory_spark.sql(
            "SELECT owner FROM test_db.task_locks WHERE lock_id = 'lock_1'"
        ).collect()
        assert len(result) == 1
        assert result[0]["owner"] == "worker_1"
