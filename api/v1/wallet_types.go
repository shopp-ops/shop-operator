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

// WalletSpec defines the desired state of Wallet
type WalletSpec struct {
	// network is the Ethereum network this wallet operates on.
	// +kubebuilder:validation:Enum=sepolia;mainnet
	// +kubebuilder:default=sepolia
	Network string `json:"network"`
}

// WalletStatus defines the observed state of Wallet.
type WalletStatus struct {
	// address is the public Ethereum address derived from the generated keypair.
	// +optional
	Address string `json:"address,omitempty"`

	// secretRef is the name of the Secret holding the private key in the same namespace.
	// +optional
	SecretRef string `json:"secretRef,omitempty"`

	// conditions represent the current state of the Wallet resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Network",type="string",JSONPath=".spec.network"
// +kubebuilder:printcolumn:name="Address",type="string",JSONPath=".status.address"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Wallet is the Schema for the wallets API
type Wallet struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec WalletSpec `json:"spec"`

	// +optional
	Status WalletStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WalletList contains a list of Wallet
type WalletList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Wallet `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Wallet{}, &WalletList{})
}
