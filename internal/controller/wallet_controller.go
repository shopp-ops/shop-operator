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

package controller

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	shopopsv1 "github.com/shopp-ops/shop-operator/api/v1"
)

// WalletReconciler reconciles a Wallet object
type WalletReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=shopops.com,resources=wallets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shopops.com,resources=wallets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shopops.com,resources=wallets/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create

func (r *WalletReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	wallet := &shopopsv1.Wallet{}
	if err := r.Get(ctx, req.NamespacedName, wallet); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	secretName := fmt.Sprintf("wallet-%s-keypair", wallet.Name)
	var address string

	existingSecret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: wallet.Namespace}, existingSecret)

	switch {
	case apierrors.IsNotFound(err):
		privateKey, genErr := crypto.GenerateKey()
		if genErr != nil {
			return ctrl.Result{}, fmt.Errorf("generating keypair: %w", genErr)
		}

		address = crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
		privateKeyHex := hex.EncodeToString(crypto.FromECDSA(privateKey))

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: wallet.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "shop-operator",
					"shopops.com/wallet":           wallet.Name,
				},
			},
			StringData: map[string]string{
				"privateKey": privateKeyHex,
				"address":    address,
			},
		}
		if createErr := r.Create(ctx, secret); createErr != nil {
			return ctrl.Result{}, fmt.Errorf("creating keypair secret: %w", createErr)
		}
		logger.Info("Ethereum keypair generated", "wallet", wallet.Name, "address", address)

	case err != nil:
		return ctrl.Result{}, fmt.Errorf("fetching keypair secret: %w", err)

	default:
		// Secret exists — it is the source of truth for the address.
		address = string(existingSecret.Data["address"])
	}

	patch := client.MergeFrom(wallet.DeepCopy())
	wallet.Status.Address = address
	wallet.Status.SecretRef = secretName
	apimeta.SetStatusCondition(&wallet.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "KeypairReady",
		Message:            "Ethereum keypair generated and stored in Secret",
		ObservedGeneration: wallet.Generation,
	})

	return ctrl.Result{}, r.Status().Patch(ctx, wallet, patch)
}

// SetupWithManager sets up the controller with the Manager.
func (r *WalletReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&shopopsv1.Wallet{}).
		Named("wallet").
		Complete(r)
}
