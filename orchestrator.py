"""
E-Commerce Pipeline Orchestrator
Automates the lifecycle: Scrape -> Validate -> Compute Deltas -> Telemetry Alert.
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Orchestrator]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PipelineOrchestrator")


class PipelineOrchestrator:
    """Controls execution flow and schema gatekeeping across pipeline modules."""

    def __init__(self, workspace_root: Optional[Path] = None):
        self.root = workspace_root or Path(__file__).resolve().parent.parent
        self.engine_dir = self.root / "shopify-catalog-engine"
        self.sentinel_dir = self.root / "catalog-validation-sentinel"
        self.delta_dir = self.root / "ecommerce-delta-engine"
        self.alerts_dir = self.root / "ecom-telemetry-alerts"
        self.local_dir = Path(__file__).resolve().parent

        # Ensure all required data directories exist across modules
        for d in [self.engine_dir, self.sentinel_dir, self.delta_dir, self.alerts_dir, self.local_dir]:
            (d / "data").mkdir(parents=True, exist_ok=True)

    def run_command(self, cmd: list, cwd: Path) -> subprocess.CompletedProcess:
        """Executes a subprocess and logs all diagnostic output on failure."""
        logger.info("Running: %s (in %s)", " ".join(cmd), cwd.name)
        result = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
        if result.returncode != 0:
            logger.error("Command failed [exit %d]", result.returncode)
            if result.stdout:
                logger.error("STDOUT:\n%s", result.stdout.strip())
            if result.stderr:
                logger.error("STDERR:\n%s", result.stderr.strip())
        return result

    def execute_pipeline(
        self,
        target_domain: str,
        webhook_url: Optional[str] = None,
        dry_run: bool = True
    ) -> Dict[str, bool]:
        """Runs the entire catalog monitoring chain."""
        status = {
            "scrape": False,
            "validate": False,
            "delta": False,
            "alert": False
        }

        # 1. Scrape snapshot via engine.py using --url
        snapshot_file = self.engine_dir / "data" / f"{target_domain}_snapshot.csv"
        scrape_cmd = [
            sys.executable,
            "engine.py",
            "--url", target_domain,
            "--output", str(snapshot_file)
        ]
        res = self.run_command(scrape_cmd, self.engine_dir)
        if res.returncode != 0 or not snapshot_file.exists():
            logger.error("Pipeline aborted: Extraction failed or snapshot missing.")
            return status
        status["scrape"] = True

        # 2. Schema Sentinel Validation Gate
        sentinel_cmd = [
            sys.executable,
            "schema_validator.py",
            "--input", str(snapshot_file)
        ]
        res = self.run_command(sentinel_cmd, self.sentinel_dir)
        if res.returncode != 0:
            logger.error("Pipeline aborted: Snapshot failed schema sentinel gate.")
            return status
        status["validate"] = True

        # 3. Delta Computation (compares baseline against new snapshot)
        baseline_file = self.delta_dir / "data" / "snapshot_day1.csv"
        delta_output = self.local_dir / "data" / "live_deltas.csv"
        delta_cmd = [
            sys.executable,
            "delta_engine.py",
            "--baseline", str(baseline_file),
            "--current", str(snapshot_file),
            "--output", str(delta_output)
        ]
        res = self.run_command(delta_cmd, self.delta_dir)
        if res.returncode != 0:
            logger.error("Pipeline aborted: Delta calculation failed.")
            return status
        status["delta"] = True

        # 4. Telemetry Alerts Dispatch
        alert_cmd = [
            sys.executable,
            "alert_dispatcher.py",
            "--deltas", str(delta_output)
        ]
        if webhook_url and not dry_run:
            alert_cmd.extend(["--webhook", webhook_url, "--send"])

        res = self.run_command(alert_cmd, self.alerts_dir)
        if res.returncode != 0:
            logger.error("Telemetry notification failed.")
            return status
        status["alert"] = True

        logger.info("Pipeline lifecycle execution completed successfully.")
        return status


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-Module Pipeline Orchestrator")
    parser.add_argument("--domain", default="gymshark", help="Target catalog domain")
    parser.add_argument("--webhook", default=None, help="Telemetry destination webhook")
    parser.add_argument("--send", action="store_true", help="Execute live webhook dispatch")
    args = parser.parse_args()

    orchestrator = PipelineOrchestrator()
    results = orchestrator.execute_pipeline(
        target_domain=args.domain,
        webhook_url=args.webhook,
        dry_run=not args.send
    )

    all_passed = all(results.values())
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()