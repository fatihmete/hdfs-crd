/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	hdfscrdv1 "fatihmete.github.io/hdfscrd/api/v1"
)

// HDFSClusterReconciler reconciles a HDFSCluster object
type HDFSClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=hdfscrd.fatihmete.github.io,resources=hdfsclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hdfscrd.fatihmete.github.io,resources=hdfsclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=hdfscrd.fatihmete.github.io,resources=hdfsclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the HDFSCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
const configHashAnnotation = "hdfs.fatihmete.github.io/config-hash"
const nodeHashAnnotation = "hdfs.fatihmete.github.io/node-hash"

func hashConfig(config *hdfscrdv1.ConfigSpec) string {

	data := config.CoreSite + config.HdfsSite
	h := sha256.New()
	h.Write([]byte(data))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func hashNode(node hdfscrdv1.HDFSNode) string {
	data := node.Name + node.Hostname + strings.Join(node.Disks, ",")
	h := sha256.New()
	h.Write([]byte(data))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func validateUniqueNamesAndHostnames(nodes []hdfscrdv1.HDFSNode) error {
	nameSet := map[string]struct{}{}
	hostnameSet := map[string]struct{}{}

	for _, node := range nodes {
		if _, exists := nameSet[node.Name]; exists {
			return fmt.Errorf("duplicate node name found: %s", node.Name)
		}
		if _, exists := hostnameSet[node.Hostname]; exists {
			return fmt.Errorf("duplicate hostname found: %s", node.Hostname)
		}
		nameSet[node.Name] = struct{}{}
		hostnameSet[node.Hostname] = struct{}{}
	}

	return nil
}

func validateNameNodeCount(nodes []hdfscrdv1.HDFSNode) error {
	count := 0
	for _, node := range nodes {
		if node.Role == "NameNode" {
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("at least one NameNode is required")
	}
	if count > 1 {
		return fmt.Errorf("only one NameNode is allowed, found %d", count)
	}
	return nil
}

func (r *HDFSClusterReconciler) CreateNodePortIfEnabled(ctx context.Context, cluster hdfscrdv1.HDFSCluster) error {

	if cluster.Spec.NodePortConfig.Enabled {
		svc := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-namenode", cluster.Name),
				Namespace: cluster.Namespace,
			},
			Spec: corev1.ServiceSpec{
				Type: corev1.ServiceTypeNodePort,
				Selector: map[string]string{
					"hdfs-role":    "namenode",
					"hdfs-cluster": cluster.Name,
				},
				Ports: []corev1.ServicePort{
					{
						Name:     "rpc",
						Port:     int32(cluster.Spec.NodePortConfig.Ports.NameNodePort),
						NodePort: int32(cluster.Spec.NodePortConfig.Ports.NodePort),
					},
				},
			},
		}
		currentNodePort := &corev1.Service{}
		err := r.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, currentNodePort)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Service does not exist, create it
				if err := r.Create(ctx, &svc); err != nil {
					logf.FromContext(ctx).Error(err, "failed to create service")
					return err
				}
			}
		}
	}
	return nil
}
func (r *HDFSClusterReconciler) RestartPodIfConfigsChanged(ctx context.Context, cluster hdfscrdv1.HDFSCluster) error {

	currentHash := hashConfig(cluster.Spec.Config)
	previousHash := cluster.Annotations[configHashAnnotation]
	if currentHash != previousHash {

		// Delete all pods associated with the cluster to force recreation
		var podList corev1.PodList
		if err := r.List(ctx, &podList, client.InNamespace(cluster.Namespace), client.MatchingLabels{"hdfsNode": "true"}); err == nil {
			for _, pod := range podList.Items {
				_ = r.Delete(ctx, &pod)
			}
		}

		// Update the config hash annotation
		if cluster.Annotations == nil {
			cluster.Annotations = map[string]string{}
		}
		cluster.Annotations[configHashAnnotation] = currentHash
		_ = r.Update(ctx, &cluster)
	}
	return nil

}

func (r *HDFSClusterReconciler) RestartPodIfNodeChanged(ctx context.Context, cluster hdfscrdv1.HDFSCluster) error {

	for _, node := range cluster.Spec.Nodes {
		currentHash := hashNode(node)
		currentPod := &corev1.Pod{}
		err := r.Get(ctx, types.NamespacedName{Name: fmt.Sprintf("%s-%s", cluster.Name, node.Name), Namespace: cluster.Namespace}, currentPod)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Pod does not exist, continue to next node
				continue
			}
			logf.FromContext(ctx).Error(err, "failed to get pod", "pod", fmt.Sprintf("%s-%s", cluster.Name, node.Name))
			return err
		}
		previousHash := currentPod.Annotations[nodeHashAnnotation]
		if currentHash != previousHash {

			// Delete the pod to force recreation
			if err := r.Delete(ctx, currentPod); err != nil {
				logf.FromContext(ctx).Error(err, "failed to delete pod", "pod", currentPod.Name)
				return err
			}
			logf.FromContext(ctx).Info("Pod deleted due to node change", "pod", currentPod.Name)
			r.CreateNodePodIfNotExists(ctx, node, cluster)

		}
	}
	return nil

}

func (r *HDFSClusterReconciler) DeletePodsIfNotExistAnymore(ctx context.Context, cluster hdfscrdv1.HDFSCluster) error {
	// List all pods in the namespace with the label matching the cluster name
	desiredPods := make(map[string]bool)
	for _, node := range cluster.Spec.Nodes {
		podName := fmt.Sprintf("%s-%s", cluster.Name, node.Name)
		desiredPods[podName] = true
	}

	// List existing pods in the namespace that match the cluster's label
	var existingPods corev1.PodList
	if err := r.List(ctx, &existingPods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"hdfsNode": "true"},
	); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	// Delete pods that are not in the current spec.nodes
	for _, pod := range existingPods.Items {
		if _, exists := desiredPods[pod.Name]; !exists {
			logf.FromContext(ctx).Info("Deleting pod not in current spec.nodes", "pod", pod.Name)
			if err := r.Delete(ctx, &pod); err != nil {
				return fmt.Errorf("failed to delete orphan pod %s: %w", pod.Name, err)
			}
		}
	}

	return nil
}

func (r *HDFSClusterReconciler) CreateHeadlessServiceIfNotExist(ctx context.Context, cluster hdfscrdv1.HDFSCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"dns": fmt.Sprintf("%s-subdomain", cluster.Name), // pod'un label’ı ile eşleşmeli
			},
			ClusterIP: "None", // Headless service
		},
	}
	currentSvc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, currentSvc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Service does not exist, create it
			if err := r.Create(ctx, svc); err != nil {
				logf.FromContext(ctx).Error(err, "failed to create service")
				return err
			}
		}
	}

	return nil
}

func (r *HDFSClusterReconciler) CreateHDFSConfigsIfNotExists(ctx context.Context, cluster hdfscrdv1.HDFSCluster) error {

	hdfsSiteXml := `<configuration>
	%s
	</configuration>`

	coreSiteXml := `<configuration>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://%s:9000</value>
	</property>
	%s
	</configuration>`

	nameNodeHostname := fmt.Sprintf("%s-namenode.%s.%s.svc.cluster.local", cluster.Name, cluster.Name, cluster.Namespace)
	// Logic to create HDFS configuration files if they do not already exist
	// This is a placeholder function. Actual implementation will depend on your Kubernetes setup.
	logf.FromContext(ctx).Info("Creating HDFS configurations")
	// You would typically use the client to create ConfigMap or other resources here.
	configMap := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-hdfs-config", cluster.Name),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"app": "hdfs-config",
			},
		},
		Data: map[string]string{
			"hdfs-site.xml": fmt.Sprintf(hdfsSiteXml, cluster.Spec.Config.HdfsSite),
			"core-site.xml": fmt.Sprintf(coreSiteXml, nameNodeHostname, cluster.Spec.Config.CoreSite),
		},
	}

	currentConfigMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMap.Name, Namespace: configMap.Namespace}, currentConfigMap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if err := controllerutil.SetOwnerReference(&cluster, &configMap, r.Scheme); err != nil {
				return err
			}
			if err := r.Create(ctx, &configMap); err != nil {
				logf.FromContext(ctx).Error(err, "failed to create configmap")
				return err
			}

			// Update the config hash annotation
			if cluster.Annotations == nil {
				cluster.Annotations = map[string]string{}
			}
			cluster.Annotations[configHashAnnotation] = hashConfig(cluster.Spec.Config)
			_ = r.Update(ctx, &cluster)

			logf.FromContext(ctx).Info("ConfigMap created successfully")
		}
	}
	return nil

}

func (r *HDFSClusterReconciler) CreateNodePodIfNotExists(ctx context.Context, node hdfscrdv1.HDFSNode, cluster hdfscrdv1.HDFSCluster) error {
	// Logic to create a DataNode pod if it does not already exist
	// This is a placeholder function. Actual implementation will depend on your Kubernetes setup.
	logf.FromContext(ctx).Info("Creating DataNode pod", "node", node.Name, "hostname", node.Hostname)
	// You would typically use the client to create a pod resource here.
	var startCommand string

	if node.Role == "NameNode" {
		startCommand = `if [ ! -d "/hadoop/nn/current" ]; then
							hdfs namenode -format -force
						fi
						hdfs namenode`

	} else if node.Role == "DataNode" {
		startCommand = "hdfs datanode"
	}

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", cluster.Name, node.Name),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				"dns":          fmt.Sprintf("%s-subdomain", cluster.Name),
				"hdfsNode":     "true",
				"hdfs-role":    strings.ToLower(node.Role),
				"hdfs-cluster": cluster.Name,
			},
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": node.Hostname,
			},
			Hostname:  fmt.Sprintf("%s-%s", cluster.Name, node.Name),
			Subdomain: cluster.Name, // This will be used for headless service
			Containers: []corev1.Container{
				{
					Name:    fmt.Sprintf("%s-%s-%s", cluster.Name, node.Name, strings.ToLower(node.Role)),
					Image:   "apache/hadoop:3.4.1",
					Command: []string{"/bin/bash", "-c"},
					Args:    []string{startCommand},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      fmt.Sprintf("%s-hdfs-config", cluster.Name),
							MountPath: "/opt/hadoop/etc/hadoop/",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: fmt.Sprintf("%s-hdfs-config", cluster.Name),
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: fmt.Sprintf("%s-hdfs-config", cluster.Name),
							},
						},
					},
				},
			},
		},
	}
	pod.Spec.InitContainers = []corev1.Container{
		{
			Name:    "cleanup-lock",
			Image:   "busybox",
			Command: []string{"sh", "-c", "find /hadoop -name 'in_use.lock' -delete"},
		}}

	i := 0
	//Set the environment variables for DataNode
	if node.Role == "DataNode" {

		pod.Spec.Containers[0].Env = []corev1.EnvVar{
			{
				Name:  "DFS_DATANODE_DATA_DIR",
				Value: strings.Join(node.Disks, ","),
			},
		}
	}
	for _, disk := range node.Disks {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: fmt.Sprintf("%s-%s-disk-%d", cluster.Name, node.Name, i),
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: disk,
					Type: func() *corev1.HostPathType {
						t := corev1.HostPathDirectoryOrCreate
						return &t
					}(),
				},
			},
		})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      fmt.Sprintf("%s-%s-disk-%d", cluster.Name, node.Name, i),
			MountPath: disk,
		})
		pod.Spec.InitContainers[0].VolumeMounts = append(pod.Spec.InitContainers[0].VolumeMounts, corev1.VolumeMount{
			Name:      fmt.Sprintf("%s-%s-disk-%d", cluster.Name, node.Name, i),
			MountPath: disk,
		})

		i++
	}

	pod.Annotations = map[string]string{
		nodeHashAnnotation: hashNode(node),
	}

	_ = controllerutil.SetOwnerReference(&cluster, &pod, r.Scheme)
	if err := controllerutil.SetControllerReference(&cluster, &pod, r.Scheme); err != nil {
		logf.FromContext(ctx).Error(err, "failed to set controller reference")
		return err
	}
	if err := r.Create(ctx, &pod); err != nil {
		logf.FromContext(ctx).Error(err, "failed to create pod")
		return err

	} else {
		logf.FromContext(ctx).Info("Pod created successfully", "pod", pod.Name, "namespace", pod.Namespace, "node", node.Name, "role", node.Role)
	}
	return nil
}

func (r *HDFSClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	_ = logf.FromContext(ctx)
	logf.FromContext(ctx).Info("Reconcile triggered", "name", req.Name, "namespace", req.Namespace)
	cluster := &hdfscrdv1.HDFSCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		logf.FromContext(ctx).Error(err, "unable to fetch HDFSCluster")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := validateUniqueNamesAndHostnames(cluster.Spec.Nodes); err != nil {
		logf.FromContext(ctx).Error(err, "Hostnames or node names are not unique")
		// Requeue=false çünkü bu bir kullanıcı hatası
		return ctrl.Result{}, err
	}

	if err := validateNameNodeCount(cluster.Spec.Nodes); err != nil {
		logf.FromContext(ctx).Error(err, "Invalid number of NameNodes")
		return ctrl.Result{}, nil
	}

	r.CreateHDFSConfigsIfNotExists(ctx, *cluster)
	r.RestartPodIfConfigsChanged(ctx, *cluster)
	r.RestartPodIfNodeChanged(ctx, *cluster)
	r.CreateHeadlessServiceIfNotExist(ctx, *cluster)
	r.CreateNodePortIfEnabled(ctx, *cluster)
	r.DeletePodsIfNotExistAnymore(ctx, *cluster)

	for _, node := range cluster.Spec.Nodes {

		podName := fmt.Sprintf("%s-%s", cluster.Name, node.Name)
		// Check if the node is existing before creating a  pod
		currentNode := corev1.Pod{}
		err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: cluster.Namespace}, &currentNode)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// If pod does not exist, create it
				logf.FromContext(ctx).Info("Pod not found, creating", "pod", podName)
				r.CreateNodePodIfNotExists(ctx, node, *cluster)
			} else {
				logf.FromContext(ctx).Error(err, "failed to get pod", "pod", podName)
				return ctrl.Result{}, err
			}
		} else {
			// Pod exists, check its status
			phase := currentNode.Status.Phase
			if phase == corev1.PodFailed || phase == corev1.PodSucceeded {
				// If the pod is in a bad phase, delete it to allow recreation
				logf.FromContext(ctx).Info("Pod in bad phase, deleting", "pod", podName, "phase", phase)
				if err := r.Delete(ctx, &currentNode); err != nil {
					logf.FromContext(ctx).Error(err, "failed to delete pod", "pod", podName)
					return ctrl.Result{}, err
				}
				// After deletion, we will recreate it in the next reconciliation loop
				logf.FromContext(ctx).Info("Pod deleted, will recreate in next reconciliation")
				return ctrl.Result{Requeue: true}, nil
			}
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HDFSClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {

	return ctrl.NewControllerManagedBy(mgr).
		For(&hdfscrdv1.HDFSCluster{}).
		Named("hdfscluster").
		Owns(&corev1.Pod{}).
		Complete(r)
}
