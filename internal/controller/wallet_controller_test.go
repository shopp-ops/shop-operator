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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	shopopsv1 "github.com/shopp-ops/shop-operator/api/v1"
)

var _ = Describe("Wallet Controller", func() {
	Context("When reconciling a Wallet resource", func() {
		const resourceName = "test-wallet"

		ctx := context.Background()

		namespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		wallet := &shopopsv1.Wallet{}

		BeforeEach(func() {
			By("creating the Wallet CR")
			err := k8sClient.Get(ctx, namespacedName, wallet)
			if err != nil && errors.IsNotFound(err) {
				resource := &shopopsv1.Wallet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: shopopsv1.WalletSpec{
						Network: "sepolia",
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			By("cleaning up the Wallet CR")
			resource := &shopopsv1.Wallet{}
			err := k8sClient.Get(ctx, namespacedName, resource)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should generate a keypair Secret and update status", func() {
			By("reconciling the Wallet")
			reconciler := &WalletReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: namespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the keypair Secret was created")
			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "wallet-test-wallet-keypair",
				Namespace: "default",
			}, secret)).To(Succeed())
			Expect(secret.Data).To(HaveKey("privateKey"))
			Expect(secret.Data).To(HaveKey("address"))

			By("verifying status is updated with the Ethereum address")
			updated := &shopopsv1.Wallet{}
			Expect(k8sClient.Get(ctx, namespacedName, updated)).To(Succeed())
			Expect(updated.Status.Address).To(HavePrefix("0x"))
			Expect(updated.Status.Address).To(HaveLen(42)) // 0x + 40 hex chars
			Expect(updated.Status.SecretRef).To(Equal("wallet-test-wallet-keypair"))

			By("verifying the Ready condition is True")
			Expect(updated.Status.Conditions).NotTo(BeEmpty())
			readyCondition := updated.Status.Conditions[0]
			Expect(readyCondition.Type).To(Equal("Ready"))
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))

			By("reconciling again to verify idempotency")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: namespacedName})
			Expect(err).NotTo(HaveOccurred())

			idempotentWallet := &shopopsv1.Wallet{}
			Expect(k8sClient.Get(ctx, namespacedName, idempotentWallet)).To(Succeed())
			Expect(idempotentWallet.Status.Address).To(Equal(updated.Status.Address))

			By("verifying the address in status matches the Secret")
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "wallet-test-wallet-keypair",
				Namespace: "default",
			}, secret)).To(Succeed())
			Expect(strings.ToLower(idempotentWallet.Status.Address)).To(
				Equal(strings.ToLower(string(secret.Data["address"]))),
			)
		})
	})
})
