/*
Copyright 2026.

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

type AvailabilityMode string

const (
	AvailabilityStandard AvailabilityMode = "standard" // 2 replicas
	AvailabilityHigh     AvailabilityMode = "high"     // 3 replicas
)

type DatabaseType string

const (
	DatabaseStandard DatabaseType = "standard" // PostgreSQL via CNPG operator
	DatabaseLight    DatabaseType = "light"    // MongoDB via MongoDB Community Operator
)

type ShopDatabase struct {
	// +kubebuilder:validation:Enum=standard;light
	Type DatabaseType `json:"type"`
}

type ShopSpec struct {
	// Display name of the shop.
	Name string `json:"name"`

	// +kubebuilder:validation:Enum=standard;high
	Availability AvailabilityMode `json:"availability"`

	// Ethereum-compatible wallet address for receiving payments.
	// +optional
	WalletAddress string `json:"walletAddress,omitempty"`

	// Database configuration.
	Database ShopDatabase `json:"database"`

	// Docker image for the shop backend application.
	ApiImage string `json:"apiImage"`

	// Docker image for the shop frontend application.
	WebImage string `json:"webImage"`

	// Ingress hostname for the shop (e.g. myshop.example.com).
	// +optional
	Host string `json:"host,omitempty"`

	// Email address for the shop admin.
	AdminEmail string `json:"adminEmail"`

	// Name of the DiscordChannel CR to use for alerts.
	// +optional
	DiscordChannelRef string `json:"discordChannelRef,omitempty"`
}

type ShopStatus struct {
	// Lifecycle phase of the Shop.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Number of available replicas.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Public URL of the shop (derived from spec.host).
	// +optional
	URL string `json:"url,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	ActiveDatabase DatabaseType `json:"activeDatabase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sh
// +kubebuilder:printcolumn:name="Availability",type=string,JSONPath=`.spec.availability`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.status.url`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// Shop is the Schema for the shops API
type Shop struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Shop
	// +required
	Spec ShopSpec `json:"spec"`

	// status defines the observed state of Shop
	// +optional
	Status ShopStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ShopList contains a list of Shop
type ShopList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Shop `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Shop{}, &ShopList{})
}
