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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// HDFSClusterSpec defines the desired state of HDFSCluster
type HDFSClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// Unique name of the HDFS cluster
	// +required
	Name *string `json:"name,omitempty"`

	// Config contains the configuration for the HDFS cluster
	Config *ConfigSpec `json:"config,omitempty"`

	// NodePortConfig defines the configuration for NodePort services
	NodePortConfig NodePortConfig `json:"nodePortConfig,omitempty"`
	// Nodes is a list of HDFS nodes in the cluster
	// Each node should have a unique name and a valid hostname
	// +kubebuilder:validation:MinItems=1
	// +required
	Nodes []HDFSNode `json:"nodes,omitempty"`
}

type NodePortConfig struct {
	Enabled bool  `json:"enabled,omitempty"`
	Ports   Ports `json:"ports,omitempty"`
}

type Ports struct {
	NameNodePort int `json:"nameNodePort,omitempty"`
	NodePort     int `json:"nodePort,omitempty"`
}

type ConfigSpec struct {
	CoreSite string `json:"coreSite,omitempty"`
	HdfsSite string `json:"hdfsSite,omitempty"`
}

type HDFSNode struct {
	// Name of the HDFS node
	// +required
	Name string `json:"name"`
	// Role of the HDFS node (e.g., NameNode, DataNode)
	// +required
	Role string `json:"role"`
	// Hostname of the HDFS node
	// +required
	Hostname string `json:"hostname"`
	// Disks is a list of disk paths used by the HDFS node
	// Each disk path should be a valid filesystem path where HDFS can store data
	// +kubebuilder:validation:MinItems=1
	// +required
	Disks []string `json:"disks"`
}

// HDFSClusterStatus defines the observed state of HDFSCluster.
type HDFSClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// HDFSCluster is the Schema for the hdfsclusters API
type HDFSCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of HDFSCluster
	// +required
	Spec HDFSClusterSpec `json:"spec"`

	// status defines the observed state of HDFSCluster
	// +optional
	Status HDFSClusterStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// HDFSClusterList contains a list of HDFSCluster
type HDFSClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HDFSCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HDFSCluster{}, &HDFSClusterList{})
}
