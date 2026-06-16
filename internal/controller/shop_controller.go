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
	"fmt"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	shopopsv1 "github.com/shopp-ops/shop-operator/api/v1"
)

const (
	shopContainerPort int32 = 8080
	servicePort       int32 = 80
)

var cnpgClusterGVK = schema.GroupVersionKind{
	Group:   "postgresql.cnpg.io",
	Version: "v1",
	Kind:    "Cluster",
}

// ShopReconciler reconciles a Shop object
//
// fieldalignment is not important for controller structs and keeping Client/Scheme
// together is the usual kubebuilder style.
//
//nolint:govet
type ShopReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=shopops.shopops.dc.com,resources=shops,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shopops.shopops.dc.com,resources=shops/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shopops.shopops.dc.com,resources=shops/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *ShopReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("shop", req.NamespacedName)
	logger.Info("Starting reconciliation")

	shop := &shopopsv1.Shop{}
	if err := r.Get(ctx, req.NamespacedName, shop); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileDatabaseSecret(ctx, shop); err != nil {
		logger.Error(err, "Failed to reconcile Secret")
		return ctrl.Result{}, err
	}

	databaseCondition, err := r.reconcileDatabase(ctx, shop)
	if err != nil {
		logger.Error(err, "Failed to reconcile database")
		return ctrl.Result{}, err
	}

	deployment, err := r.reconcileDeployment(ctx, shop)
	if err != nil {
		logger.Error(err, "Failed to reconcile Deployment")
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, shop); err != nil {
		logger.Error(err, "Failed to reconcile Service")
		return ctrl.Result{}, err
	}

	if err := r.reconcileIngress(ctx, shop); err != nil {
		logger.Error(err, "Failed to reconcile Ingress")
		return ctrl.Result{}, err
	}

	if err := r.reconcileStatus(ctx, shop, deployment, databaseCondition); err != nil {
		logger.Error(err, "Failed to reconcile status")
		return ctrl.Result{}, err
	}

	logger.Info("Finished reconciliation")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ShopReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&shopopsv1.Shop{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&networkingv1.Ingress{}).
		Named("shop").
		Complete(r)
}

func (r *ShopReconciler) reconcileDatabaseSecret(ctx context.Context, shop *shopopsv1.Shop) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.databaseSecretName(shop), Namespace: shop.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if err := controllerutil.SetControllerReference(shop, secret, r.Scheme); err != nil {
			return err
		}

		if secret.Labels == nil {
			secret.Labels = map[string]string{}
		}
		for key, value := range r.labelsForShop(shop) {
			secret.Labels[key] = value
		}

		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}

		if len(secret.Data["password"]) == 0 {
			secret.Data["password"] = []byte(fmt.Sprintf("%s-password", shop.Name))
		}

		secret.Type = corev1.SecretTypeOpaque
		secret.Data["username"] = []byte("shop")
		secret.Data["database"] = []byte("shop")
		secret.Data["host"] = []byte(r.databaseReadWriteServiceName(shop))
		secret.Data["port"] = []byte("5432")

		return nil
	})

	return err
}

func (r *ShopReconciler) reconcileDatabase(ctx context.Context, shop *shopopsv1.Shop) (metav1.Condition, error) {
	if shop.Spec.Database.Type != shopopsv1.DatabaseStandard {
		if err := r.deleteDatabaseCluster(ctx, shop); err != nil {
			return metav1.Condition{}, err
		}

		return metav1.Condition{
			Type:               "DatabaseReady",
			Status:             metav1.ConditionFalse,
			Reason:             "UnsupportedDatabaseType",
			Message:            fmt.Sprintf("Database type %q is not implemented yet", shop.Spec.Database.Type),
			ObservedGeneration: shop.Generation,
		}, nil
	}

	cluster := r.desiredDatabaseCluster(shop)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cluster, func() error {
		if err := controllerutil.SetControllerReference(shop, cluster, r.Scheme); err != nil {
			return err
		}

		cluster.Object["metadata"] = map[string]any{
			"name":      cluster.GetName(),
			"namespace": cluster.GetNamespace(),
			"labels":    r.labelsForShop(shop),
		}
		cluster.Object["spec"] = map[string]any{
			"instances": 1,
			"bootstrap": map[string]any{
				"initdb": map[string]any{
					"database": "shop",
					"owner":    "shop",
					"secret": map[string]any{
						"name": r.databaseSecretName(shop),
					},
				},
			},
			"storage": map[string]any{
				"size": "1Gi",
			},
		}
		return nil
	})
	if err != nil {
		if apimeta.IsNoMatchError(err) {
			return metav1.Condition{
				Type:               "DatabaseReady",
				Status:             metav1.ConditionFalse,
				Reason:             "DatabaseCRDMissing",
				Message:            "CloudNativePG Cluster CRD is not installed in the target cluster",
				ObservedGeneration: shop.Generation,
			}, nil
		}

		return metav1.Condition{}, err
	}

	return metav1.Condition{
		Type:               "DatabaseReady",
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "Database Cluster is reconciled",
		ObservedGeneration: shop.Generation,
	}, nil
}

func (r *ShopReconciler) deleteDatabaseCluster(ctx context.Context, shop *shopopsv1.Shop) error {
	cluster := r.desiredDatabaseCluster(shop)
	if err := r.Delete(ctx, cluster); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			return nil
		}
		return err
	}

	return nil
}

func (r *ShopReconciler) reconcileDeployment(ctx context.Context, shop *shopopsv1.Shop) (*appsv1.Deployment, error) {
	replicas := r.desiredReplicas(shop)
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: shop.Name, Namespace: shop.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if err := controllerutil.SetControllerReference(shop, deployment, r.Scheme); err != nil {
			return err
		}

		deployment.Labels = r.labelsForShop(shop)
		deployment.Spec.Replicas = &replicas
		deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: r.selectorLabelsForShop(shop)}
		deployment.Spec.Template.ObjectMeta.Labels = r.selectorLabelsForShop(shop)
		deployment.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            "shop",
			Image:           shop.Spec.Image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: shopContainerPort,
			}},
			Env: []corev1.EnvVar{
				{Name: "SHOP_NAME", Value: r.displayName(shop)},
				{Name: "WALLET_ADDRESS", Value: r.walletAddress(shop)},
				{Name: "DISCORD_CHANNEL", Value: r.discordChannel(shop)},
				{Name: "DATABASE_TYPE", Value: string(shop.Spec.Database.Type)},
				{
					Name: "DATABASE_HOST",
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: r.databaseSecretName(shop)},
						Key:                  "host",
					}},
				},
				{
					Name: "DATABASE_PORT",
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: r.databaseSecretName(shop)},
						Key:                  "port",
					}},
				},
				{
					Name: "DATABASE_NAME",
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: r.databaseSecretName(shop)},
						Key:                  "database",
					}},
				},
				{
					Name: "DATABASE_USER",
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: r.databaseSecretName(shop)},
						Key:                  "username",
					}},
				},
				{
					Name: "DATABASE_PASSWORD",
					ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: r.databaseSecretName(shop)},
						Key:                  "password",
					}},
				},
			},
		}}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return deployment, nil
}

func (r *ShopReconciler) reconcileService(ctx context.Context, shop *shopopsv1.Shop) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: r.serviceName(shop), Namespace: shop.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := controllerutil.SetControllerReference(shop, service, r.Scheme); err != nil {
			return err
		}

		service.Labels = r.labelsForShop(shop)
		service.Spec.Selector = r.selectorLabelsForShop(shop)
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       servicePort,
			TargetPort: intstr.FromInt32(shopContainerPort),
		}}

		return nil
	})

	return err
}

func (r *ShopReconciler) reconcileIngress(ctx context.Context, shop *shopopsv1.Shop) error {
	ingress := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: r.ingressName(shop), Namespace: shop.Namespace}}

	if shop.Spec.Host == "" {
		if err := r.Delete(ctx, ingress); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	className := "nginx"
	pathType := networkingv1.PathTypePrefix
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ingress, func() error {
		if err := controllerutil.SetControllerReference(shop, ingress, r.Scheme); err != nil {
			return err
		}

		ingress.Labels = r.labelsForShop(shop)
		ingress.Spec.IngressClassName = &className
		ingress.Spec.Rules = []networkingv1.IngressRule{{
			Host: shop.Spec.Host,
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path:     "/",
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{
								Name: r.serviceName(shop),
								Port: networkingv1.ServiceBackendPort{Number: servicePort},
							},
						},
					}},
				},
			},
		}}

		return nil
	})

	return err
}

func (r *ShopReconciler) reconcileStatus(ctx context.Context, shop *shopopsv1.Shop, deployment *appsv1.Deployment, databaseCondition metav1.Condition) error {
	latest := &shopopsv1.Shop{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(shop), latest); err != nil {
		return err
	}

	originalStatus := latest.Status.DeepCopy()
	desiredReplicas := r.desiredReplicas(shop)
	availableReplicas := deployment.Status.AvailableReplicas

	latest.Status.Replicas = availableReplicas
	latest.Status.URL = r.shopURL(shop)

	readyCondition := metav1.Condition{
		Type:               "Available",
		Status:             metav1.ConditionFalse,
		Reason:             "Progressing",
		Message:            fmt.Sprintf("Waiting for %d replicas to become available", desiredReplicas),
		ObservedGeneration: latest.Generation,
	}
	if availableReplicas >= desiredReplicas {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "Available"
		readyCondition.Message = "Deployment is available"
	}

	apimeta.SetStatusCondition(&latest.Status.Conditions, readyCondition)
	apimeta.SetStatusCondition(&latest.Status.Conditions, databaseCondition)

	switch {
	case readyCondition.Status == metav1.ConditionTrue && databaseCondition.Status == metav1.ConditionTrue:
		latest.Status.Phase = "Ready"
	case databaseCondition.Status == metav1.ConditionFalse && (databaseCondition.Reason == "DatabaseCRDMissing" || databaseCondition.Reason == "UnsupportedDatabaseType"):
		latest.Status.Phase = "Degraded"
	default:
		latest.Status.Phase = "Progressing"
	}

	if reflect.DeepEqual(*originalStatus, latest.Status) {
		return nil
	}

	return r.Status().Update(ctx, latest)
}

func (r *ShopReconciler) desiredDatabaseCluster(shop *shopopsv1.Shop) *unstructured.Unstructured {
	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(cnpgClusterGVK)
	cluster.SetName(r.databaseClusterName(shop))
	cluster.SetNamespace(shop.Namespace)
	return cluster
}

func (r *ShopReconciler) desiredReplicas(shop *shopopsv1.Shop) int32 {
	if shop.Spec.Availability == shopopsv1.AvailabilityHigh {
		return 3
	}

	return 2
}

func (r *ShopReconciler) labelsForShop(shop *shopopsv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "shop",
		"app.kubernetes.io/instance":   shop.Name,
		"app.kubernetes.io/managed-by": "shop-operator",
		"shopops.shopops.dc.com/shop":  shop.Name,
	}
}

func (r *ShopReconciler) selectorLabelsForShop(shop *shopopsv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "shop",
		"app.kubernetes.io/instance":  shop.Name,
		"shopops.shopops.dc.com/shop": shop.Name,
	}
}

func (r *ShopReconciler) displayName(shop *shopopsv1.Shop) string {
	if shop.Spec.Name != "" {
		return shop.Spec.Name
	}

	return shop.Name
}

func (r *ShopReconciler) walletAddress(shop *shopopsv1.Shop) string {
	if shop.Spec.WalletAddress != "" {
		return shop.Spec.WalletAddress
	}

	return "0x0000000000000000000000000000000000000000"
}

func (r *ShopReconciler) discordChannel(shop *shopopsv1.Shop) string {
	if shop.Spec.DiscordChannelRef != "" {
		return shop.Spec.DiscordChannelRef
	}

	return "shop-alerts"
}

func (r *ShopReconciler) serviceName(shop *shopopsv1.Shop) string {
	return shop.Name
}

func (r *ShopReconciler) ingressName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-ingress", shop.Name)
}

func (r *ShopReconciler) databaseSecretName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-db-auth", shop.Name)
}

func (r *ShopReconciler) databaseClusterName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-db", shop.Name)
}

func (r *ShopReconciler) databaseReadWriteServiceName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-rw", r.databaseClusterName(shop))
}

func (r *ShopReconciler) shopURL(shop *shopopsv1.Shop) string {
	if shop.Spec.Host == "" {
		return ""
	}

	return fmt.Sprintf("https://%s", shop.Spec.Host)
}

func namespacedName(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}
