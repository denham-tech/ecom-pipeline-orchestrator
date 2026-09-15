"""
Unit tests for Pipeline Orchestrator
Verifies subprocess coordination, path resolution, and failure gating.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
from orchestrator import PipelineOrchestrator


@pytest.fixture
def orchestrator(tmp_path):
    # Mock workspace root structure
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "shopify-catalog-engine" / "data").mkdir(parents=True)
    (root / "catalog-validation-sentinel").mkdir()
    (root / "ecommerce-delta-engine" / "data").mkdir(parents=True)
    (root / "ecom-telemetry-alerts").mkdir()
    
    # Seed mock baseline snapshot
    baseline = root / "ecommerce-delta-engine" / "data" / "snapshot_day1.csv"
    baseline.write_text("id,title,price,available\n1,Shoe,50.0,True\n")

    return PipelineOrchestrator(workspace_root=root)


def test_orchestrator_initializes_paths(orchestrator):
    assert orchestrator.engine_dir.name == "shopify-catalog-engine"
    assert orchestrator.sentinel_dir.name == "catalog-validation-sentinel"
    assert orchestrator.delta_dir.name == "ecommerce-delta-engine"
    assert orchestrator.alerts_dir.name == "ecom-telemetry-alerts"


@patch("subprocess.run")
def test_pipeline_execution_success(mock_run, orchestrator):
    # Fake successful execution for all 4 stages
    mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
    
    # Create the simulated scraped output file so orchestrator continues
    target_snapshot = orchestrator.engine_dir / "data" / "teststore_snapshot.csv"
    target_snapshot.write_text("id,title,price,available\n1,Shoe,60.0,True\n")

    results = orchestrator.execute_pipeline(
        target_domain="teststore",
        webhook_url=None,
        dry_run=True
    )

    assert results["scrape"] is True
    assert results["validate"] is True
    assert results["delta"] is True
    assert results["alert"] is True
    assert mock_run.call_count == 4


@patch("subprocess.run")
def test_pipeline_aborts_on_validation_failure(mock_run, orchestrator):
    # Step 1 (scrape) passes, Step 2 (validate) fails
    target_snapshot = orchestrator.engine_dir / "data" / "teststore_snapshot.csv"
    target_snapshot.write_text("corrupted,data\n")

    mock_run.side_effect = [
        MagicMock(returncode=0, stdout="Scrape complete"),  # Scrape
        MagicMock(returncode=1, stderr="Validation error")   # Sentinel gate failure
    ]

    results = orchestrator.execute_pipeline(
        target_domain="teststore",
        dry_run=True
    )

    assert results["scrape"] is True
    assert results["validate"] is False
    assert results["delta"] is False
    assert results["alert"] is False
    assert mock_run.call_count == 2