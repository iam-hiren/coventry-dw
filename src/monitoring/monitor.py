"""Monitoring and alerting module for the Coventry DW pipeline."""

import json
import smtplib
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

from ..utils import get_logger, config

logger = get_logger(__name__)


class PipelineMonitor:
    """Monitors pipeline execution and sends alerts."""
    
    def __init__(self):
        self.monitoring_config = config.get_monitoring_config()
        self.status_file = Path("pipeline_status.json")
        self.metrics_file = Path("pipeline_metrics.json")
        
    def record_pipeline_run(self, run_id: str, pipeline_name: str, status: str, 
                          metadata: Dict[str, Any]) -> None:
        """Record pipeline run status and metrics."""
        logger.info(f"Recording pipeline run: {run_id}", pipeline_name=pipeline_name, status=status)
        
        run_record = {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata
        }
        
        # Update status file
        self._update_status_file(run_record)
        
        # Update metrics
        self._update_metrics(run_record)
        
        # Send alerts if needed
        if status in ['failed', 'completed_with_warnings']:
            self._send_alerts(run_record)
    
    def _update_status_file(self, run_record: Dict[str, Any]) -> None:
        """Update the pipeline status file."""
        try:
            # Load existing status
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    status_data = json.load(f)
            else:
                status_data = {"runs": []}
            
            # Add new run record
            status_data["runs"].append(run_record)
            
            # Keep only last 100 runs
            status_data["runs"] = status_data["runs"][-100:]
            
            # Update summary
            status_data["last_updated"] = datetime.utcnow().isoformat()
            status_data["total_runs"] = len(status_data["runs"])
            
            # Calculate success rate
            recent_runs = status_data["runs"][-20:]  # Last 20 runs
            successful_runs = sum(1 for run in recent_runs if run["status"] == "completed")
            status_data["success_rate"] = successful_runs / len(recent_runs) if recent_runs else 0
            
            # Save updated status
            with open(self.status_file, 'w') as f:
                json.dump(status_data, f, indent=2, default=str)
                
        except Exception as e:
            logger.error("Failed to update status file", error=str(e))
    
    def _update_metrics(self, run_record: Dict[str, Any]) -> None:
        """Update pipeline metrics."""
        try:
            # Load existing metrics
            if self.metrics_file.exists():
                with open(self.metrics_file, 'r') as f:
                    metrics_data = json.load(f)
            else:
                metrics_data = {
                    "daily_metrics": {},
                    "pipeline_metrics": {},
                    "performance_metrics": []
                }
            
            # Update daily metrics
            today = datetime.utcnow().date().isoformat()
            if today not in metrics_data["daily_metrics"]:
                metrics_data["daily_metrics"][today] = {
                    "total_runs": 0,
                    "successful_runs": 0,
                    "failed_runs": 0,
                    "total_rows_processed": 0,
                    "total_processing_time": 0
                }
            
            daily_metrics = metrics_data["daily_metrics"][today]
            daily_metrics["total_runs"] += 1
            
            if run_record["status"] == "completed":
                daily_metrics["successful_runs"] += 1
            elif run_record["status"] == "failed":
                daily_metrics["failed_runs"] += 1
            
            # Extract processing metrics from metadata
            metadata = run_record.get("metadata", {})
            if "total_rows_processed" in metadata:
                daily_metrics["total_rows_processed"] += metadata["total_rows_processed"]
            if "duration_seconds" in metadata:
                daily_metrics["total_processing_time"] += metadata["duration_seconds"]
            
            # Update pipeline-specific metrics
            pipeline_name = run_record["pipeline_name"]
            if pipeline_name not in metrics_data["pipeline_metrics"]:
                metrics_data["pipeline_metrics"][pipeline_name] = {
                    "total_runs": 0,
                    "avg_processing_time": 0,
                    "last_successful_run": None,
                    "consecutive_failures": 0
                }
            
            pipeline_metrics = metrics_data["pipeline_metrics"][pipeline_name]
            pipeline_metrics["total_runs"] += 1
            
            if run_record["status"] == "completed":
                pipeline_metrics["last_successful_run"] = run_record["timestamp"]
                pipeline_metrics["consecutive_failures"] = 0
            elif run_record["status"] == "failed":
                pipeline_metrics["consecutive_failures"] += 1
            
            # Update performance metrics
            if "duration_seconds" in metadata:
                performance_record = {
                    "timestamp": run_record["timestamp"],
                    "pipeline_name": pipeline_name,
                    "duration_seconds": metadata["duration_seconds"],
                    "rows_processed": metadata.get("total_rows_processed", 0),
                    "status": run_record["status"]
                }
                metrics_data["performance_metrics"].append(performance_record)
                
                # Keep only last 1000 performance records
                metrics_data["performance_metrics"] = metrics_data["performance_metrics"][-1000:]
            
            # Save updated metrics
            with open(self.metrics_file, 'w') as f:
                json.dump(metrics_data, f, indent=2, default=str)
                
        except Exception as e:
            logger.error("Failed to update metrics", error=str(e))
    
    def _send_alerts(self, run_record: Dict[str, Any]) -> None:
        """Send alerts based on pipeline status."""
        if not self.monitoring_config.get("enabled", True):
            return
        
        alerts_config = self.monitoring_config.get("alerts", {})
        
        # Email alerts
        if alerts_config.get("email", {}).get("enabled", False):
            self._send_email_alert(run_record, alerts_config["email"])
        
        # Slack alerts
        if alerts_config.get("slack", {}).get("enabled", False):
            self._send_slack_alert(run_record, alerts_config["slack"])
    
    def _send_email_alert(self, run_record: Dict[str, Any], email_config: Dict[str, Any]) -> None:
        """Send email alert."""
        try:
            # Email configuration from environment
            smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
            smtp_port = int(os.getenv("SMTP_PORT", "587"))
            email_user = os.getenv("EMAIL_USER")
            email_password = os.getenv("EMAIL_PASSWORD")
            alert_email = os.getenv("ALERT_EMAIL")
            
            if not all([email_user, email_password, alert_email]):
                logger.warning("Email configuration incomplete, skipping email alert")
                return
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = email_user
            msg['To'] = alert_email
            msg['Subject'] = f"Coventry DW Pipeline Alert - {run_record['status'].upper()}"
            
            # Email body
            body = self._create_alert_message(run_record)
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(email_user, email_password)
            text = msg.as_string()
            server.sendmail(email_user, alert_email, text)
            server.quit()
            
            logger.info("Email alert sent successfully", run_id=run_record["run_id"])
            
        except Exception as e:
            logger.error("Failed to send email alert", error=str(e))
    
    def _send_slack_alert(self, run_record: Dict[str, Any], slack_config: Dict[str, Any]) -> None:
        """Send Slack alert."""
        try:
            webhook_url = os.getenv("SLACK_WEBHOOK_URL")
            
            if not webhook_url:
                logger.warning("Slack webhook URL not configured, skipping Slack alert")
                return
            
            # Create Slack message
            status = run_record["status"]
            color = "good" if status == "completed" else "danger"
            
            message = {
                "attachments": [
                    {
                        "color": color,
                        "title": f"Coventry DW Pipeline - {status.upper()}",
                        "fields": [
                            {
                                "title": "Pipeline",
                                "value": run_record["pipeline_name"],
                                "short": True
                            },
                            {
                                "title": "Run ID",
                                "value": run_record["run_id"],
                                "short": True
                            },
                            {
                                "title": "Timestamp",
                                "value": run_record["timestamp"],
                                "short": True
                            },
                            {
                                "title": "Status",
                                "value": status,
                                "short": True
                            }
                        ]
                    }
                ]
            }
            
            # Add error details if failed
            if status == "failed" and "error" in run_record.get("metadata", {}):
                message["attachments"][0]["fields"].append({
                    "title": "Error",
                    "value": run_record["metadata"]["error"][:500],  # Truncate long errors
                    "short": False
                })
            
            # Send to Slack
            response = requests.post(webhook_url, json=message)
            response.raise_for_status()
            
            logger.info("Slack alert sent successfully", run_id=run_record["run_id"])
            
        except Exception as e:
            logger.error("Failed to send Slack alert", error=str(e))
    
    def _create_alert_message(self, run_record: Dict[str, Any]) -> str:
        """Create alert message content."""
        message = f"""
Coventry Building Society Data Warehouse Pipeline Alert

Pipeline: {run_record['pipeline_name']}
Run ID: {run_record['run_id']}
Status: {run_record['status'].upper()}
Timestamp: {run_record['timestamp']}

"""
        
        metadata = run_record.get("metadata", {})
        
        if "total_rows_processed" in metadata:
            message += f"Rows Processed: {metadata['total_rows_processed']:,}\n"
        
        if "duration_seconds" in metadata:
            duration_minutes = metadata['duration_seconds'] / 60
            message += f"Duration: {duration_minutes:.2f} minutes\n"
        
        if run_record['status'] == "failed" and "error" in metadata:
            message += f"\nError Details:\n{metadata['error']}\n"
        
        if "sources_processed" in metadata:
            message += f"\nSources Processed: {len(metadata['sources_processed'])}\n"
            for source in metadata['sources_processed']:
                message += f"  - {source['source_name']}: {source['status']}\n"
        
        message += "\nPlease check the pipeline logs for more details."
        
        return message
    
    def get_pipeline_health(self) -> Dict[str, Any]:
        """Get overall pipeline health status."""
        try:
            if not self.status_file.exists():
                return {"status": "unknown", "message": "No pipeline runs recorded"}
            
            with open(self.status_file, 'r') as f:
                status_data = json.load(f)
            
            runs = status_data.get("runs", [])
            if not runs:
                return {"status": "unknown", "message": "No pipeline runs recorded"}
            
            # Get recent runs (last 24 hours)
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            recent_runs = [
                run for run in runs 
                if datetime.fromisoformat(run["timestamp"].replace('Z', '+00:00')) > cutoff_time
            ]
            
            if not recent_runs:
                return {"status": "stale", "message": "No recent pipeline runs"}
            
            # Calculate health metrics
            total_recent = len(recent_runs)
            successful_recent = sum(1 for run in recent_runs if run["status"] == "completed")
            failed_recent = sum(1 for run in recent_runs if run["status"] == "failed")
            
            success_rate = successful_recent / total_recent if total_recent > 0 else 0
            
            # Determine overall health
            if success_rate >= 0.9:
                health_status = "healthy"
            elif success_rate >= 0.7:
                health_status = "warning"
            else:
                health_status = "critical"
            
            # Get last run status
            last_run = runs[-1]
            last_run_status = last_run["status"]
            
            return {
                "status": health_status,
                "success_rate": success_rate,
                "recent_runs": total_recent,
                "successful_runs": successful_recent,
                "failed_runs": failed_recent,
                "last_run_status": last_run_status,
                "last_run_time": last_run["timestamp"],
                "message": f"Pipeline health: {health_status} (Success rate: {success_rate:.1%})"
            }
            
        except Exception as e:
            logger.error("Failed to get pipeline health", error=str(e))
            return {"status": "error", "message": f"Error checking health: {str(e)}"}
    
    def export_metrics(self, output_path: Optional[str] = None) -> Path:
        """Export metrics to file."""
        if output_path is None:
            output_path = f"pipeline_metrics_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        output_file = Path(output_path)
        
        try:
            # Combine all metrics
            export_data = {
                "export_timestamp": datetime.utcnow().isoformat(),
                "pipeline_health": self.get_pipeline_health()
            }
            
            # Add status data
            if self.status_file.exists():
                with open(self.status_file, 'r') as f:
                    export_data["status_data"] = json.load(f)
            
            # Add metrics data
            if self.metrics_file.exists():
                with open(self.metrics_file, 'r') as f:
                    export_data["metrics_data"] = json.load(f)
            
            # Save export
            with open(output_file, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            logger.info(f"Metrics exported to: {output_file}")
            return output_file
            
        except Exception as e:
            logger.error("Failed to export metrics", error=str(e))
            raise


def main():
    """Main entry point for monitoring utilities."""
    monitor = PipelineMonitor()
    
    # Get and display pipeline health
    health = monitor.get_pipeline_health()
    print(f"Pipeline Health Status: {health['status']}")
    print(f"Message: {health['message']}")
    
    # Export metrics
    export_file = monitor.export_metrics()
    print(f"Metrics exported to: {export_file}")


if __name__ == "__main__":
    main()
