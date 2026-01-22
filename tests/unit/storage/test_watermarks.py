"""Tests for watermark tracking functionality.

This module tests the watermark tracking capabilities of the MemoryStorageManager,
which enables incremental processing tests without a real Spark cluster.
"""

from datetime import datetime

import pytest

from sparkless.storage.backends.memory import MemoryStorageManager


@pytest.fixture
def storage_manager():
    """Create a fresh MemoryStorageManager for each test."""
    manager = MemoryStorageManager()
    yield manager
    # Cleanup
    manager.reset_watermarks()


@pytest.mark.unit
class TestWatermarkTracking:
    """Test suite for watermark tracking functionality."""

    def test_set_and_get_watermark(self, storage_manager):
        """Test setting and getting a watermark."""
        # Set watermark
        storage_manager.set_watermark("task_1", "2024-01-01 00:00:00")

        # Get watermark
        wm = storage_manager.get_watermark("task_1")
        assert wm is not None
        assert wm["value"] == "2024-01-01 00:00:00"
        assert "updated_at" in wm
        assert isinstance(wm["updated_at"], datetime)
        assert wm["metadata"] == {}

    def test_watermark_with_metadata(self, storage_manager):
        """Test watermark with metadata."""
        storage_manager.set_watermark(
            "task_2",
            "2024-01-15",
            metadata={"org_id": "org-123", "table": "sales"},
        )
        wm = storage_manager.get_watermark("task_2")
        assert wm is not None
        assert wm["value"] == "2024-01-15"
        assert wm["metadata"]["org_id"] == "org-123"
        assert wm["metadata"]["table"] == "sales"

    def test_get_nonexistent_watermark(self, storage_manager):
        """Test getting a watermark that doesn't exist."""
        wm = storage_manager.get_watermark("nonexistent_task")
        assert wm is None

    def test_update_existing_watermark(self, storage_manager):
        """Test updating an existing watermark."""
        # Set initial watermark
        storage_manager.set_watermark("task_update", "2024-01-01")
        initial_wm = storage_manager.get_watermark("task_update")
        assert initial_wm is not None
        initial_time = initial_wm["updated_at"]

        # Update watermark
        storage_manager.set_watermark(
            "task_update",
            "2024-02-01",
            metadata={"updated": True},
        )
        updated_wm = storage_manager.get_watermark("task_update")
        assert updated_wm is not None
        assert updated_wm["value"] == "2024-02-01"
        assert updated_wm["metadata"]["updated"] is True
        assert updated_wm["updated_at"] >= initial_time

    def test_delete_watermark(self, storage_manager):
        """Test deleting a watermark."""
        storage_manager.set_watermark("task_3", "2024-02-01")
        assert storage_manager.get_watermark("task_3") is not None

        result = storage_manager.delete_watermark("task_3")
        assert result is True
        assert storage_manager.get_watermark("task_3") is None

    def test_delete_nonexistent_watermark(self, storage_manager):
        """Test deleting a watermark that doesn't exist."""
        result = storage_manager.delete_watermark("nonexistent_task")
        assert result is False

    def test_list_watermarks(self, storage_manager):
        """Test listing all watermarks."""
        storage_manager.reset_watermarks()
        storage_manager.set_watermark("task_a", "v1")
        storage_manager.set_watermark("task_b", "v2")

        wms = storage_manager.list_watermarks()
        assert len(wms) == 2
        assert "task_a" in wms
        assert "task_b" in wms
        assert wms["task_a"]["value"] == "v1"
        assert wms["task_b"]["value"] == "v2"

    def test_list_watermarks_returns_copy(self, storage_manager):
        """Test that list_watermarks returns a copy, not the internal dict."""
        storage_manager.set_watermark("task_copy", "value")
        wms = storage_manager.list_watermarks()

        # Modifying the returned dict shouldn't affect internal state
        wms["task_copy"]["value"] = "modified"
        assert storage_manager.get_watermark("task_copy")["value"] == "value"

    def test_reset_watermarks(self, storage_manager):
        """Test resetting all watermarks."""
        storage_manager.set_watermark("task_x", "value")
        storage_manager.set_watermark("task_y", "value")
        assert len(storage_manager.list_watermarks()) == 2

        storage_manager.reset_watermarks()
        assert len(storage_manager.list_watermarks()) == 0

    def test_watermark_with_different_value_types(self, storage_manager):
        """Test watermarks with different value types."""
        # String (timestamp)
        storage_manager.set_watermark("task_str", "2024-01-01T00:00:00Z")
        assert (
            storage_manager.get_watermark("task_str")["value"] == "2024-01-01T00:00:00Z"
        )

        # Integer (sequence number)
        storage_manager.set_watermark("task_int", 12345)
        assert storage_manager.get_watermark("task_int")["value"] == 12345

        # Float (unix timestamp)
        storage_manager.set_watermark("task_float", 1704067200.0)
        assert storage_manager.get_watermark("task_float")["value"] == 1704067200.0

        # Datetime object
        dt = datetime(2024, 1, 1, 0, 0, 0)
        storage_manager.set_watermark("task_dt", dt)
        assert storage_manager.get_watermark("task_dt")["value"] == dt

    def test_watermark_isolation_between_tasks(self, storage_manager):
        """Test that watermarks are properly isolated between tasks."""
        storage_manager.set_watermark("task_isolated_1", "value_1", {"key": "val_1"})
        storage_manager.set_watermark("task_isolated_2", "value_2", {"key": "val_2"})

        wm1 = storage_manager.get_watermark("task_isolated_1")
        wm2 = storage_manager.get_watermark("task_isolated_2")

        assert wm1["value"] == "value_1"
        assert wm1["metadata"]["key"] == "val_1"
        assert wm2["value"] == "value_2"
        assert wm2["metadata"]["key"] == "val_2"

        # Delete one shouldn't affect the other
        storage_manager.delete_watermark("task_isolated_1")
        assert storage_manager.get_watermark("task_isolated_1") is None
        assert storage_manager.get_watermark("task_isolated_2") is not None


@pytest.mark.unit
class TestWatermarkWithSparkSession:
    """Test watermark tracking through SparkSession."""

    def test_watermark_via_storage_manager(self, spark):
        """Test watermark access via SparkSession's storage manager."""
        # Access storage manager from spark session
        storage = spark._storage

        # Set watermark
        storage.set_watermark("spark_task_1", "2024-01-01 00:00:00")

        # Get watermark
        wm = storage.get_watermark("spark_task_1")
        assert wm["value"] == "2024-01-01 00:00:00"

        # Cleanup
        storage.reset_watermarks()

    def test_watermark_persistence_across_operations(self, spark):
        """Test that watermarks persist across DataFrame operations."""
        storage = spark._storage
        storage.reset_watermarks()

        # Set initial watermark
        storage.set_watermark("incremental_task", "2024-01-01")

        # Perform some DataFrame operations
        df = spark.createDataFrame([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])
        df.count()

        # Watermark should still be there
        wm = storage.get_watermark("incremental_task")
        assert wm is not None
        assert wm["value"] == "2024-01-01"

        # Cleanup
        storage.reset_watermarks()


@pytest.mark.unit
class TestWatermarkWithSQLMerge:
    """Test watermark update via SQL MERGE operations."""

    def test_watermark_table_create_and_query(self, spark):
        """Test creating and querying a watermarks table via SQL."""
        # Create watermarks table
        spark.sql("""
            CREATE TABLE test_db.watermarks (
                task_id STRING,
                watermark_value STRING,
                updated_at TIMESTAMP
            )
        """)

        # Insert initial watermark
        spark.sql("""
            INSERT INTO test_db.watermarks
            VALUES ('task_1', '2024-01-01', current_timestamp())
        """)

        # Verify
        result = spark.sql("""
            SELECT watermark_value
            FROM test_db.watermarks
            WHERE task_id = 'task_1'
        """).collect()
        assert len(result) == 1
        assert result[0]["watermark_value"] == "2024-01-01"

    def test_watermark_table_update(self, spark):
        """Test updating watermark via SQL UPDATE."""
        # Create and populate table
        spark.sql("""
            CREATE TABLE test_db.watermarks_update (
                task_id STRING,
                watermark_value STRING
            )
        """)
        spark.sql("""
            INSERT INTO test_db.watermarks_update
            VALUES ('task_1', '2024-01-01')
        """)

        # Update using INSERT (simulating overwrite)
        spark.sql("""
            INSERT INTO test_db.watermarks_update
            VALUES ('task_1', '2024-02-01')
        """)

        # Verify the table has both values (append mode)
        result = spark.sql("""
            SELECT watermark_value
            FROM test_db.watermarks_update
            WHERE task_id = 'task_1'
            ORDER BY watermark_value DESC
        """).collect()
        assert len(result) == 2
        # Latest value should be first after descending sort
        assert result[0]["watermark_value"] == "2024-02-01"

    def test_watermark_integration_scenario(self, spark):
        """Test a realistic incremental processing scenario.

        This test demonstrates how watermarks can be used to track
        incremental processing state alongside SQL operations.
        """
        storage = spark._storage
        storage.reset_watermarks()

        # Simulate incremental task
        task_id = "bronze_to_silver_sales"

        # First run: no watermark exists
        wm = storage.get_watermark(task_id)
        assert wm is None

        # Create source data table
        spark.sql("""
            CREATE TABLE test_db.sales_bronze (
                id STRING,
                amount DOUBLE,
                created_at STRING
            )
        """)
        spark.sql("""
            INSERT INTO test_db.sales_bronze VALUES
            ('1', 100.0, '2024-01-01'),
            ('2', 200.0, '2024-01-02'),
            ('3', 300.0, '2024-01-03')
        """)

        # Process first batch (simulate first run - process all)
        result = spark.sql("SELECT * FROM test_db.sales_bronze")
        first_batch_count = result.count()
        assert first_batch_count == 3

        # Set watermark after first run
        storage.set_watermark(
            task_id,
            "2024-01-03",
            metadata={"rows_processed": first_batch_count, "batch": 1},
        )

        # Verify watermark was set
        wm = storage.get_watermark(task_id)
        assert wm is not None
        assert wm["value"] == "2024-01-03"
        assert wm["metadata"]["rows_processed"] == 3

        # Add new data (simulating new records arriving)
        spark.sql("""
            INSERT INTO test_db.sales_bronze VALUES
            ('4', 400.0, '2024-01-04'),
            ('5', 500.0, '2024-01-05')
        """)

        # Second run: read watermark to know where we left off
        wm = storage.get_watermark(task_id)
        assert wm is not None
        last_watermark = wm["value"]
        assert last_watermark == "2024-01-03"

        # In a real scenario, we would filter based on the watermark
        # For this test, we verify total records grew
        result = spark.sql("SELECT * FROM test_db.sales_bronze")
        total_count = result.count()
        assert total_count == 5

        # Simulate processing only new records (2 new records)
        new_records_processed = total_count - wm["metadata"]["rows_processed"]
        assert new_records_processed == 2

        # Update watermark after second batch
        storage.set_watermark(
            task_id,
            "2024-01-05",
            metadata={
                "rows_processed": new_records_processed,
                "cumulative_rows": total_count,
                "batch": 2,
            },
        )

        # Verify final watermark state
        final_wm = storage.get_watermark(task_id)
        assert final_wm["value"] == "2024-01-05"
        assert final_wm["metadata"]["cumulative_rows"] == 5
        assert final_wm["metadata"]["batch"] == 2

        # Cleanup
        storage.reset_watermarks()
