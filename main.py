import kopf
import kubernetes
import logging
import os
import yaml

LABEL_NAME = os.getenv('LABEL_NAME', 'maintenanceApproved')
# Fleet clusters are mostly in fleet-default, but we search all
MAINTENANCEWINDOW_GROUP = os.getenv('MAINTENANCEWINDOW_GROUP', 'maintenance.fleet.io')
MAINTENANCEWINDOW_VERSION = os.getenv('MAINTENANCEWINDOW_VERSION', 'v1alpha1')

FINALIZER_NAME = "maintenance.fleet.io/cleanup"

def get_cluster_names(spec):
    clusters = spec.get('clusters', [])
    if not clusters:
        logging.warning("No clusters specified in MaintenanceWindow spec.")
    return clusters

def set_maintenance_label(cluster_name: str, active: str = "false"):
    api = kubernetes.client.CustomObjectsApi()
    try:
        # 1. Find the cluster in provisioning.cattle.io to get its namespace
        prov_group = "provisioning.cattle.io"
        prov_version = "v1"
        plural = "clusters"
        
        clusters = api.list_cluster_custom_object(prov_group, prov_version, plural)
        target_cluster = None
        for item in clusters.get('items', []):
            if item['metadata']['name'] == cluster_name:
                target_cluster = item
                break
        
        if not target_cluster:
            logging.error(f"Cluster {cluster_name} not found in provisioning.cattle.io")
            return

        target_ns = target_cluster['metadata']['namespace']
        labels = target_cluster.get('metadata', {}).get('labels', {})
        labels[LABEL_NAME] = active
        
        body = {"metadata": {"labels": {LABEL_NAME: active}}}
        
        # 2. Patch Provisioning cluster
        api.patch_namespaced_custom_object(
            group=prov_group,
            version=prov_version,
            namespace=target_ns,
            plural=plural,
            name=cluster_name,
            body=body
        )
        logging.info(f"Patched provisioning cluster '{cluster_name}' in '{target_ns}' to {active}")

    except Exception as e:
        logging.error(f"Error in set_maintenance_label for cluster '{cluster_name}': {e}")

@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    # Enable Peering (Leader Election) for High Availability (HA)
    settings.peering.name = "maintenance-operator-peering"
    settings.peering.namespace = os.getenv("FLEET_NAMESPACE", getattr(settings.peering, 'namespace', None))
    # Decrease timeouts for faster leader takeover
    settings.peering.priority = 100
    
    try:
        if os.environ.get('KUBERNETES_SERVICE_HOST'):
            kubernetes.config.load_incluster_config()
            logging.info("Loaded in-cluster Kubernetes config.")
        else:
            kubernetes.config.load_kube_config()
            logging.info("Loaded kubeconfig.")
    except Exception as e:
        logging.error(f"Error loading Kubernetes config: {e}")

@kopf.on.create(MAINTENANCEWINDOW_GROUP, MAINTENANCEWINDOW_VERSION, 'maintenancewindows')
@kopf.on.update(MAINTENANCEWINDOW_GROUP, MAINTENANCEWINDOW_VERSION, 'maintenancewindows')
def manage_finalizer(patch, body, **kwargs):
    if FINALIZER_NAME not in body.get('metadata', {}).get('finalizers', []):
        logging.info(f"Adding finalizer {FINALIZER_NAME} to {body['metadata']['name']}")
        patch.setdefault('metadata', {}).setdefault('finalizers', []).append(FINALIZER_NAME)

@kopf.on.delete(MAINTENANCEWINDOW_GROUP, MAINTENANCEWINDOW_VERSION, 'maintenancewindows')
def cleanup_on_delete(body, spec, status, **kwargs):
    logging.info(f"Cleanup triggered for delete: {body['metadata']['name']}")
    
    # Prioritize clusters actually resolved by the controller
    resolved = status.get('resolvedClusters', [])
    if resolved:
        logging.info(f"Cleaning up {len(resolved)} resolved clusters from status.")
        for cluster_info in resolved:
            set_maintenance_label(cluster_info['name'], "false")
    else:
        # Fallback to spec if status is empty (unlikely but safer)
        logging.info("Status has no resolved clusters. Falling back to spec clusters.")
        clusters = get_cluster_names(spec)
        for cluster_name in clusters:
            set_maintenance_label(cluster_name, "false")
            
    logging.info(f"Completed cleanup for {body['metadata']['name']}")
