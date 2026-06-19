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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	shopopsv1 "github.com/shopp-ops/shop-operator/api/v1"
)

var _ = Describe("Shop Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Shop")
			resource := &shopopsv1.Shop{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				return
			}
			Expect(errors.IsNotFound(err)).To(BeTrue())

			resource = &shopopsv1.Shop{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: shopopsv1.ShopSpec{
					Name:              "My Test Shop",
					Availability:      shopopsv1.AvailabilityStandard,
					WalletAddress:     "0x1111111111111111111111111111111111111111",
					ApiImage:          "nginx:latest",
					WebImage:          "nginx:latest",
					Host:              "test-shop.example.com",
					DiscordChannelRef: "shop-alerts",
					Database: shopopsv1.ShopDatabase{
						Type: shopopsv1.DatabaseStandard,
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &shopopsv1.Shop{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if errors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())

			By("cleaning up the specific resource instance Shop")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource and create owned resources", func() {
			By("reconciling the created resource")
			controllerReconciler := &ShopReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			apiDeployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-api"), apiDeployment)).To(Succeed())
			Expect(*apiDeployment.Spec.Replicas).To(Equal(int32(2)))
			Expect(apiDeployment.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(apiDeployment.Spec.Template.Spec.Containers[0].Image).To(Equal("nginx:latest"))

			webDeployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-web"), webDeployment)).To(Succeed())

			apiService := &corev1.Service{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-api"), apiService)).To(Succeed())
			Expect(apiService.Spec.Ports).To(HaveLen(1))
			Expect(apiService.Spec.Ports[0].Port).To(Equal(int32(80)))

			webService := &corev1.Service{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-web"), webService)).To(Succeed())

			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-app-secret"), secret)).To(Succeed())
			Expect(secret.Data).To(HaveKeyWithValue("username", []byte("shop")))
			Expect(secret.Data).To(HaveKeyWithValue("password", []byte("test-resource-password")))
			Expect(secret.Data).To(HaveKey("admin-email"))
			Expect(secret.Data).To(HaveKey("jwt-secret"))

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, namespacedName("default", "test-resource-ingress"), ingress)).To(Succeed())
			Expect(ingress.Spec.Rules).To(HaveLen(1))
			Expect(ingress.Spec.Rules[0].Host).To(Equal("test-shop.example.com"))

			shop := &shopopsv1.Shop{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, shop)).To(Succeed())
			Expect(shop.Status.URL).To(Equal("https://test-shop.example.com"))
			Expect(shop.Status.Phase).NotTo(BeEmpty())
		})
	})
})
