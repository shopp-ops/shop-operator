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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	shopopsv1 "github.com/shopp-ops/shop-operator/api/v1"
)

var _ = Describe("Shop Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		var adminEmail string
		var walletAddress string
		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			adminEmail = ""
			walletAddress = "0x1111111111111111111111111111111111111111"
		})

		JustBeforeEach(func() {
			By("creating the custom resource for the Kind Shop")

			for _, obj := range []client.Object{
				&shopopsv1.Shop{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"}},
				&shopopsv1.Wallet{ObjectMeta: metav1.ObjectMeta{Name: "test-resource-wallet", Namespace: "default"}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test-resource-app-secret", Namespace: "default"}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test-resource-admin-credentials", Namespace: "default"}},
			} {
				err := k8sClient.Delete(ctx, obj)
				if err != nil && !errors.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred())
				}
			}

			resource := &shopopsv1.Shop{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: shopopsv1.ShopSpec{
					Name:              "My Test Shop",
					Availability:      shopopsv1.AvailabilityStandard,
					WalletAddress:     walletAddress,
					ApiImage:          "nginx:latest",
					WebImage:          "nginx:latest",
					Host:              "test-shop.example.com",
					AdminEmail:        adminEmail,
					DiscordChannelRef: "shop-alerts",
					Database: shopopsv1.ShopDatabase{
						Type: shopopsv1.DatabaseStandard,
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			By("cleaning up the specific resource instance Shop")
			for _, obj := range []client.Object{
				&shopopsv1.Shop{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"}},
				&shopopsv1.Wallet{ObjectMeta: metav1.ObjectMeta{Name: "test-resource-wallet", Namespace: "default"}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test-resource-app-secret", Namespace: "default"}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test-resource-admin-credentials", Namespace: "default"}},
			} {
				err := k8sClient.Delete(ctx, obj)
				if err != nil && !errors.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred())
				}
			}
		})

		It("should gracefully report missing database CRDs in envtest", func() {
			By("reconciling the created resource")
			controllerReconciler := &ShopReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-app-secret"), secret)).To(Succeed())
			Expect(secret.Data).To(HaveKeyWithValue("username", []byte("shop")))
			Expect(secret.Data).To(HaveKeyWithValue("password", []byte("test-resource-password")))
			Expect(secret.Data).To(HaveKey("jwt-secret"))
			Expect(secret.Data).To(HaveKey("postgres-url"))
			Expect(secret.Data).To(HaveKey("database-url"))

			adminSecret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-admin-credentials"), adminSecret)).To(Succeed())
			Expect(adminSecret.Data).To(HaveKeyWithValue("admin-email", []byte("admin@shop.local")))
			Expect(adminSecret.Data).To(HaveKey("admin-password"))
			Expect(adminSecret.Data["admin-password"]).NotTo(BeEmpty())

			shop := &shopopsv1.Shop{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, shop)).To(Succeed())
			Expect(shop.Status.URL).To(Equal("https://test-shop.example.com"))
			Expect(shop.Status.Phase).To(Equal("Degraded"))
			Expect(shop.Status.ActiveDatabase).To(Equal(shopopsv1.DatabaseStandard))
			Expect(shop.Status.WalletAddress).To(Equal(walletAddress))

			walletCondition := apimeta.FindStatusCondition(shop.Status.Conditions, "WalletReady")
			Expect(walletCondition).NotTo(BeNil())
			Expect(walletCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(walletCondition.Reason).To(Equal("Provided"))

			wallet := &shopopsv1.Wallet{}
			err = k8sClient.Get(ctx, namespacedName("default", "test-resource-wallet"), wallet)
			Expect(errors.IsNotFound(err)).To(BeTrue())

			databaseCondition := apimeta.FindStatusCondition(shop.Status.Conditions, "DatabaseReady")
			Expect(databaseCondition).NotTo(BeNil())
			Expect(databaseCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(databaseCondition.Reason).To(Equal("DatabaseCRDMissing"))

			availableCondition := apimeta.FindStatusCondition(shop.Status.Conditions, "Available")
			Expect(availableCondition).NotTo(BeNil())
			Expect(availableCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(availableCondition.Reason).To(Equal("WaitingForDatabase"))

			apiService := &corev1.Service{}
			err = k8sClient.Get(ctx, namespacedName("default", "test-resource-api"), apiService)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		Context("when wallet address is omitted from spec", func() {
			BeforeEach(func() {
				walletAddress = ""
			})

			It("should create a Wallet resource without owner references", func() {
				controllerReconciler := &ShopReconciler{
					Client: k8sClient,
					Scheme: k8sClient.Scheme(),
				}

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
				Expect(err).NotTo(HaveOccurred())

				wallet := &shopopsv1.Wallet{}
				Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-wallet"), wallet)).To(Succeed())
				Expect(wallet.Spec.Network).To(Equal("sepolia"))
				Expect(wallet.OwnerReferences).To(BeEmpty())
				Expect(wallet.Annotations).To(HaveKeyWithValue("shopops.com/created-for-shop", "test-resource"))
				Expect(wallet.Labels).To(HaveKeyWithValue("shopops.com/shop", "test-resource"))

				shop := &shopopsv1.Shop{}
				Expect(k8sClient.Get(ctx, typeNamespacedName, shop)).To(Succeed())
				Expect(shop.Status.WalletAddress).To(BeEmpty())

				walletCondition := apimeta.FindStatusCondition(shop.Status.Conditions, "WalletReady")
				Expect(walletCondition).NotTo(BeNil())
				Expect(walletCondition.Status).To(Equal(metav1.ConditionFalse))
				Expect(walletCondition.Reason).To(Equal("WalletCreating"))

				apiService := &corev1.Service{}
				err = k8sClient.Get(ctx, namespacedName("default", "test-resource-api"), apiService)
				Expect(errors.IsNotFound(err)).To(BeTrue())
			})
		})

		Context("when admin email is set in spec", func() {
			BeforeEach(func() {
				adminEmail = "owner@test-shop.example.com"
			})

			It("should use admin email from spec when creating admin secret", func() {
				controllerReconciler := &ShopReconciler{
					Client: k8sClient,
					Scheme: k8sClient.Scheme(),
				}

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
				Expect(err).NotTo(HaveOccurred())

				adminSecret := &corev1.Secret{}
				Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-admin-credentials"), adminSecret)).To(Succeed())
				Expect(adminSecret.Data).To(HaveKeyWithValue("admin-email", []byte("owner@test-shop.example.com")))
				Expect(adminSecret.Data).To(HaveKey("admin-password"))
				Expect(adminSecret.Data["admin-password"]).NotTo(BeEmpty())
			})
		})
	})
})
