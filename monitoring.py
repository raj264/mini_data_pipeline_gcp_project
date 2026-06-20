"""Module: monitoring.py
Implement Cloud Monitoring checks and Pub/Sub alerts."""
from google.cloud import monitoring_v3, pubsub_v1

def create_uptime_check(project_id: str, display_name: str, monitored_uri: str):
    """Create an uptime check in Cloud Monitoring."""
    client = monitoring_v3.UptimeCheckServiceClient()
    parent = f"projects/{project_id}"
    config = monitoring_v3.UptimeCheckConfig(
        display_name=display_name,
        monitored_resource=monitoring_v3.MonitoredResource(type_='uptime_url'),
        http_check=monitoring_v3.UptimeCheckConfig.HttpCheck(
            path=monitored_uri, port=443, use_ssl=True),
        timeout=10
    )
    return client.create_uptime_check_config(parent=parent, uptime_check_config=config)

def publish_alert(topic: str, message: str):
    """Publish an alert message to Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    publisher.publish(topic, message.encode('utf-8'))
