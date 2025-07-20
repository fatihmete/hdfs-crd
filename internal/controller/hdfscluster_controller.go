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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	if err := r.Create(ctx, svc); err != nil {
		logf.FromContext(ctx).Error(err, "failed to create service")
		return err
	}
	return nil
}

func (r *HDFSClusterReconciler) CreateHDFSConfigsIfNotExists(ctx context.Context, cluster hdfscrdv1.HDFSCluster) error {

	hdfsSiteXml := `<configuration>
    <!-- file system properties -->
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/hadoop/nn</value>
        <description>Determines where on the local filesystem the DFS name node
            should store the name table. If this is a comma-delimited list
            of directories then the name table is replicated in all of the
            directories, for redundancy. </description>
        <final>true</final>
    </property>

    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/hadoop/disk1</value>
        <description>Determines where on the local filesystem an DFS data node
            should store its blocks. If this is a comma-delimited
            list of directories, then data will be stored in all named
            directories, typically on different devices.
            Directories that do not exist are ignored.
        </description>
        <final>true</final>
    </property>

    <property>
        <name>dfs.webhdfs.enable</name>
        <value>true</value>
    </property>
    <property>
        <name>dfs.http.address</name>
        <value>0.0.0.0:50470</value>
        <final>true</final>
    </property>
	</configuration>`

	coreSiteXml := `<configuration>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://%s:9000</value>
	</property>
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
			"hdfs-site.xml": hdfsSiteXml,
			"core-site.xml": fmt.Sprintf(coreSiteXml, nameNodeHostname),
		},
	}
	if err := controllerutil.SetOwnerReference(&cluster, &configMap, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, &configMap); err != nil {
		logf.FromContext(ctx).Error(err, "failed to create configmap")
		return err
	}
	logf.FromContext(ctx).Info("ConfigMap created successfully")
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
				"dns": fmt.Sprintf("%s-subdomain", cluster.Name),
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
	for _, disk := range node.Disks {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: fmt.Sprintf("%s-%s-disk", cluster.Name, node.Name),
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
			Name:      fmt.Sprintf("%s-%s-disk", cluster.Name, node.Name),
			MountPath: disk,
		})
	}

	_ = controllerutil.SetOwnerReference(&cluster, &pod, r.Scheme)
	if err := r.Create(ctx, &pod); err != nil {
		logf.FromContext(ctx).Error(err, "failed to create pod")
		return err

	} else {
		logf.FromContext(ctx).Info("POD CREATED!!!!")
	}
	return nil
}

func (r *HDFSClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)
	cluster := &hdfscrdv1.HDFSCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		logf.FromContext(ctx).Error(err, "unable to fetch HDFSCluster")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	r.CreateHDFSConfigsIfNotExists(ctx, *cluster)
	r.CreateHeadlessServiceIfNotExist(ctx, *cluster)

	for _, node := range cluster.Spec.Nodes {

		podName := fmt.Sprintf("%s-%s", cluster.Name, node.Name)
		// Check if the node is existing before creating a  pod
		currentNode := corev1.Pod{}
		err := r.Get(ctx, types.NamespacedName{Name: podName}, &currentNode)
		if err != nil {
			r.CreateNodePodIfNotExists(ctx, node, *cluster)
		}
		// logf.FromContext(ctx).Info("Processing HDFS Node", "name", node.Name, "role", node.Role, "hostname", node.Hostname)
		//for _, disk := range
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *HDFSClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&hdfscrdv1.HDFSCluster{}).
		Named("hdfscluster").
		Complete(r)
}
