package controller

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"time"

	shopopsv1 "github.com/shopp-ops/shop-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
)

const (
	apiContainerPort int32 = 3000
	webContainerPort int32 = 3000
	servicePort      int32 = 80

	mongoAppDatabase      = "shop"
	mongoAuthDB           = "admin"
	mongoUsername         = "shop"
	mongoDatabaseSAName   = "mongodb-database"
	mongoDatabaseRoleName = "mongodb-database"
	mongoDatabaseRBName   = "mongodb-database"
)

var (
	cnpgClusterGVK = schema.GroupVersionKind{
		Group:   "postgresql.cnpg.io",
		Version: "v1",
		Kind:    "Cluster",
	}
	mongoDBCommunityGVK = schema.GroupVersionKind{
		Group:   "mongodbcommunity.mongodb.com",
		Version: "v1",
		Kind:    "MongoDBCommunity",
	}
)

type deploymentConfig struct {
	name      string
	image     string
	port      int32
	labels    map[string]string
	selectors map[string]string
	env       []corev1.EnvVar
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
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgresql.cnpg.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mongodbcommunity.mongodb.com,resources=mongodbcommunity,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *ShopReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("shop", req.NamespacedName)
	logger.Info("Starting reconciliation")

	shop := &shopopsv1.Shop{}
	if err := r.Get(ctx, req.NamespacedName, shop); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileAppSecretBase(ctx, shop); err != nil {
		logger.Error(err, "Failed to reconcile Secret")
		return ctrl.Result{}, err
	}

	databaseURL, activeDB, databaseCondition, err := r.reconcileDatabase(ctx, shop)
	if err != nil {
		logger.Error(err, "Failed to reconcile database")
		return ctrl.Result{}, err
	}

	if err := r.reconcileAppSecretDatabaseURL(ctx, shop, databaseURL); err != nil {
		logger.Error(err, "Failed to update database URL in Secret")
		return ctrl.Result{}, err
	}

	if databaseCondition.Status != metav1.ConditionTrue {
		logger.Info("Database is not ready yet, skipping Deployment reconciliation", "reason", databaseCondition.Reason)

		if err := r.reconcileStatus(ctx, shop, nil, databaseCondition, activeDB); err != nil {
			logger.Error(err, "Failed to reconcile status")
			return ctrl.Result{}, err
		}

		switch databaseCondition.Reason {
		case "DatabaseConnectionPending":
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		case "MigrationInProgress", "MigrationPending":
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		default:
			return ctrl.Result{}, nil
		}

	}

	apiDeployment, err := r.reconcileDeployment(ctx, shop, deploymentConfig{
		name:      r.apiDeploymentName(shop),
		image:     shop.Spec.ApiImage,
		port:      apiContainerPort,
		labels:    r.labelsForApi(shop),
		selectors: r.selectorLabelsForApi(shop),
		env:       r.apiEnvVars(shop),
	})
	if err != nil {
		logger.Error(err, "Failed to reconcile API Deployment")
		return ctrl.Result{}, err
	}

	if _, err := r.reconcileDeployment(ctx, shop, deploymentConfig{
		name:      r.webDeploymentName(shop),
		image:     shop.Spec.WebImage,
		port:      webContainerPort,
		labels:    r.labelsForWeb(shop),
		selectors: r.selectorLabelsForWeb(shop),
	}); err != nil {
		logger.Error(err, "Failed to reconcile Web Deployment")
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, shop, r.apiServiceName(shop), apiContainerPort, r.labelsForApi(shop), r.selectorLabelsForApi(shop)); err != nil {
		logger.Error(err, "Failed to reconcile API Service")
		return ctrl.Result{}, err
	}

	if err := r.reconcileService(ctx, shop, r.webServiceName(shop), webContainerPort, r.labelsForWeb(shop), r.selectorLabelsForWeb(shop)); err != nil {
		logger.Error(err, "Failed to reconcile Web Service")
		return ctrl.Result{}, err
	}

	if err := r.reconcileIngress(ctx, shop); err != nil {
		logger.Error(err, "Failed to reconcile Ingress")
		return ctrl.Result{}, err
	}

	if err := r.reconcileStatus(ctx, shop, apiDeployment, databaseCondition, activeDB); err != nil {
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
		Owns(&batchv1.Job{}).
		Named("shop").
		Complete(r)
}

func (r *ShopReconciler) reconcileAppSecretBase(ctx context.Context, shop *shopopsv1.Shop) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.appSecretName(shop), Namespace: shop.Namespace}}

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

		if len(secret.Data["db-password"]) == 0 {
			secret.Data["db-password"] = []byte(fmt.Sprintf("%s-password", shop.Name))
		}
		if len(secret.Data["admin-password"]) == 0 {
			secret.Data["admin-password"] = []byte("changeme")
		}
		if len(secret.Data["jwt-secret"]) == 0 {
			secret.Data["jwt-secret"] = []byte("change-me-in-production")
		}
		if len(secret.Data["admin-email"]) == 0 {
			secret.Data["admin-email"] = []byte("admin@shop.local")
		}

		secret.Type = corev1.SecretTypeOpaque
		secret.Data["username"] = []byte("shop")
		secret.Data["password"] = secret.Data["db-password"]

		pgURL := fmt.Sprintf("postgresql://shop:%s@%s:5432/shop",
			string(secret.Data["db-password"]),
			r.databaseReadWriteServiceName(shop))
		secret.Data["postgres-url"] = []byte(pgURL)

		mongoURL, ready, err := r.mongoConnectionString(ctx, shop)
		if err != nil {
			return err
		}
		if ready {
			secret.Data["mongo-url"] = []byte(mongoURL)
		} else {
			delete(secret.Data, "mongo-url")
		}

		return nil
	})

	return err
}

func (r *ShopReconciler) reconcileAppSecretDatabaseURL(ctx context.Context, shop *shopopsv1.Shop, databaseURL string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.appSecretName(shop), Namespace: shop.Namespace}}

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

		secret.Type = corev1.SecretTypeOpaque
		if databaseURL == "" {
			return nil
		}

		secret.Data["database-url"] = []byte(databaseURL)
		return nil
	})

	return err
}

func (r *ShopReconciler) appSecretName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-app-secret", shop.Name)
}

func (r *ShopReconciler) reconcileDatabase(ctx context.Context, shop *shopopsv1.Shop) (string, shopopsv1.DatabaseType, metav1.Condition, error) {
	currentType := shop.Spec.Database.Type
	recordedActiveType := shop.Status.ActiveDatabase
	activeType := recordedActiveType
	if activeType == "" {
		activeType = currentType
	}

	activeURL, err := r.databaseURLFor(ctx, shop, activeType)
	if err != nil {
		return "", activeType, metav1.Condition{}, err
	}

	needsMigration := recordedActiveType != "" && recordedActiveType != currentType

	switch currentType {
	case shopopsv1.DatabaseStandard:
		condition, err := r.reconcilePostgresDatabase(ctx, shop)
		if err != nil || condition.Status != metav1.ConditionTrue {
			return activeURL, activeType, condition, err
		}

		if needsMigration {
			completed, migrationCondition, err := r.reconcileDatabaseMigration(
				ctx,
				shop,
				"mongo",
				"postgres",
				func(ctx context.Context, shop *shopopsv1.Shop) (metav1.Condition, error) {
					_, condition, err := r.reconcileMongoDatabase(ctx, shop)
					return condition, err
				},
				func(ctx context.Context, shop *shopopsv1.Shop) error {
					if err := r.deleteMongoDatabase(ctx, shop); err != nil {
						return err
					}
					return r.deleteMongoRBAC(ctx, shop)
				},
			)
			if err != nil {
				return activeURL, activeType, metav1.Condition{}, err
			}
			if !completed {
				return activeURL, activeType, migrationCondition, nil
			}
		}

		url, err := r.postgresDatabaseURL(ctx, shop)
		return url, shopopsv1.DatabaseStandard, condition, err

	case shopopsv1.DatabaseLight:
		_, condition, err := r.reconcileMongoDatabase(ctx, shop)
		if err != nil || condition.Status != metav1.ConditionTrue {
			return activeURL, activeType, condition, err
		}

		if needsMigration {
			completed, migrationCondition, err := r.reconcileDatabaseMigration(
				ctx,
				shop,
				"postgres",
				"mongo",
				func(ctx context.Context, shop *shopopsv1.Shop) (metav1.Condition, error) {
					return r.reconcilePostgresDatabase(ctx, shop)
				},
				func(ctx context.Context, shop *shopopsv1.Shop) error {
					return r.deleteDatabaseCluster(ctx, shop)
				},
			)
			if err != nil {
				return activeURL, activeType, metav1.Condition{}, err
			}
			if !completed {
				return activeURL, activeType, migrationCondition, nil
			}
		}

		url, err := r.reconcileMongoDatabaseURL(ctx, shop)
		return url, shopopsv1.DatabaseLight, condition, err

	default:
		return activeURL, activeType, metav1.Condition{
			Type:               "DatabaseReady",
			Status:             metav1.ConditionFalse,
			Reason:             "UnsupportedDatabaseType",
			Message:            fmt.Sprintf("Database type %q is not implemented yet", currentType),
			ObservedGeneration: shop.Generation,
		}, nil
	}
}

func (r *ShopReconciler) reconcileDatabaseMigration(
	ctx context.Context,
	shop *shopopsv1.Shop,
	from string,
	to string,
	ensureSource func(context.Context, *shopopsv1.Shop) (metav1.Condition, error),
	cleanup func(context.Context, *shopopsv1.Shop) error,
) (bool, metav1.Condition, error) {
	sourceCondition, err := ensureSource(ctx, shop)
	if err != nil {
		return false, metav1.Condition{}, err
	}
	if sourceCondition.Status != metav1.ConditionTrue {
		return false, r.databasePendingCondition(shop, "Waiting for source MongoDB to be ready before migration"), nil
	}

	if err := r.reconcileAppSecretBase(ctx, shop); err != nil {
		return false, metav1.Condition{}, err
	}

	urlsReady, err := r.migrationURLsReady(ctx, shop, from, to)
	if err != nil {
		return false, metav1.Condition{}, err
	}
	if !urlsReady {
		return false, r.databasePendingCondition(shop, "Waiting for migration connection URLs to be populated"), nil
	}

	done, failureCondition, err := r.reconcileMigrationJob(ctx, shop, from, to)
	if err != nil {
		return false, metav1.Condition{}, err
	}
	if failureCondition != nil {
		return false, *failureCondition, nil
	}
	if !done {
		return false, r.databaseInProgressCondition(shop, "Migrating data from MongoDB to PostgreSQL"), nil
	}

	if err := cleanup(ctx, shop); err != nil {
		return false, metav1.Condition{}, err
	}

	return true, metav1.Condition{}, nil
}

func (r *ShopReconciler) databasePendingCondition(shop *shopopsv1.Shop, message string) metav1.Condition {
	return metav1.Condition{
		Type:               "DatabaseReady",
		Status:             metav1.ConditionFalse,
		Reason:             "MigrationPending",
		Message:            message,
		ObservedGeneration: shop.Generation,
	}
}

func (r *ShopReconciler) databaseInProgressCondition(shop *shopopsv1.Shop, message string) metav1.Condition {
	return metav1.Condition{
		Type:               "DatabaseReady",
		Status:             metav1.ConditionFalse,
		Reason:             "MigrationInProgress",
		Message:            message,
		ObservedGeneration: shop.Generation,
	}
}

func (r *ShopReconciler) reconcilePostgresDatabase(ctx context.Context, shop *shopopsv1.Shop) (metav1.Condition, error) {
	cluster := r.desiredDatabaseCluster(shop)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cluster, func() error {
		if err := controllerutil.SetControllerReference(shop, cluster, r.Scheme); err != nil {
			return err
		}

		existingLabels := cluster.GetLabels()
		if existingLabels == nil {
			existingLabels = map[string]string{}
		}
		for k, v := range r.labelsForShop(shop) {
			existingLabels[k] = v
		}
		cluster.SetLabels(existingLabels)

		cluster.Object["spec"] = map[string]any{
			"instances": 1,
			"bootstrap": map[string]any{
				"initdb": map[string]any{
					"database": "shop",
					"owner":    "shop",
					"secret": map[string]any{
						"name": r.appSecretName(shop),
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
		Message:            "PostgreSQL Cluster is reconciled",
		ObservedGeneration: shop.Generation,
	}, nil
}

func (r *ShopReconciler) reconcileMongoDatabase(ctx context.Context, shop *shopopsv1.Shop) (string, metav1.Condition, error) {
	if err := r.reconcileMongoRBAC(ctx, shop); err != nil {
		return "", metav1.Condition{}, err
	}

	mongo := r.desiredMongoDatabase(shop)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, mongo, func() error {
		if err := controllerutil.SetControllerReference(shop, mongo, r.Scheme); err != nil {
			return err
		}

		existingLabels := mongo.GetLabels()
		if existingLabels == nil {
			existingLabels = map[string]string{}
		}
		for k, v := range r.labelsForShop(shop) {
			existingLabels[k] = v
		}
		mongo.SetLabels(existingLabels)

		mongo.Object["spec"] = map[string]any{
			"members": 1,
			"type":    "ReplicaSet",
			"version": "6.0.5",
			"security": map[string]any{
				"authentication": map[string]any{
					"modes": []any{"SCRAM"},
				},
			},
			"users": []any{
				map[string]any{
					"name":                       mongoUsername,
					"db":                         mongoAuthDB,
					"passwordSecretRef":          map[string]any{"name": r.appSecretName(shop)},
					"connectionStringSecretName": r.mongoConnectionSecretName(shop),
					"roles": []any{
						map[string]any{"name": "readWrite", "db": mongoAppDatabase},
					},
					"scramCredentialsSecretName": r.mongoSCRAMSecretName(shop),
				},
			},
		}
		return nil
	})
	if err != nil {
		if apimeta.IsNoMatchError(err) {
			return "", metav1.Condition{
				Type:               "DatabaseReady",
				Status:             metav1.ConditionFalse,
				Reason:             "DatabaseCRDMissing",
				Message:            "MongoDBCommunity CRD is not installed in the target cluster",
				ObservedGeneration: shop.Generation,
			}, nil
		}

		return "", metav1.Condition{}, err
	}

	connectionString, ready, err := r.mongoConnectionString(ctx, shop)
	if err != nil {
		return "", metav1.Condition{}, err
	}
	if !ready {
		return "", metav1.Condition{
			Type:               "DatabaseReady",
			Status:             metav1.ConditionFalse,
			Reason:             "DatabaseConnectionPending",
			Message:            fmt.Sprintf("Waiting for MongoDB connection secret %q or %q", r.mongoConnectionSecretName(shop), r.mongoDefaultConnectionSecretName(shop)),
			ObservedGeneration: shop.Generation,
		}, nil
	}

	return connectionString, metav1.Condition{
		Type:               "DatabaseReady",
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "MongoDB resource is reconciled",
		ObservedGeneration: shop.Generation,
	}, nil
}

func (r *ShopReconciler) mongoConnectionString(ctx context.Context, shop *shopopsv1.Shop) (string, bool, error) {
	for _, secretName := range []string{r.mongoConnectionSecretName(shop), r.mongoDefaultConnectionSecretName(shop)} {
		secret := &corev1.Secret{}
		err := r.Get(ctx, namespacedName(shop.Namespace, secretName), secret)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", false, err
		}

		for _, key := range []string{"connectionString.standard", "connectionString.standardSrv"} {
			if value, ok := secret.Data[key]; ok && len(value) > 0 {

				connectionString := string(value)

				u, err := url.Parse(connectionString)
				if err != nil {
					return "", false, err
				}

				u.Path = "/" + mongoAppDatabase

				q := u.Query()
				q.Set("authSource", mongoAuthDB)
				u.RawQuery = q.Encode()

				return u.String(), true, nil
			}
		}
	}

	return "", false, nil
}

func (r *ShopReconciler) postgresDatabaseURL(ctx context.Context, shop *shopopsv1.Shop) (string, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, namespacedName(shop.Namespace, r.appSecretName(shop)), secret); err != nil {
		return "", err
	}

	password := secret.Data["db-password"]
	if len(password) == 0 {
		password = secret.Data["password"]
	}

	return fmt.Sprintf("postgresql://shop:%s@%s:5432/shop", string(password), r.databaseReadWriteServiceName(shop)), nil
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

func (r *ShopReconciler) deleteMongoDatabase(ctx context.Context, shop *shopopsv1.Shop) error {
	mongo := r.desiredMongoDatabase(shop)
	if err := r.Delete(ctx, mongo); err != nil {
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) {
			return nil
		}
		return err
	}

	return nil
}

func (r *ShopReconciler) reconcileMongoRBAC(ctx context.Context, shop *shopopsv1.Shop) error {
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: mongoDatabaseSAName, Namespace: shop.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, serviceAccount, func() error {
		if err := controllerutil.SetControllerReference(shop, serviceAccount, r.Scheme); err != nil {
			return err
		}
		serviceAccount.Labels = r.labelsForShop(shop)
		return nil
	}); err != nil {
		return err
	}

	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: mongoDatabaseRoleName, Namespace: shop.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		if err := controllerutil.SetControllerReference(shop, role, r.Scheme); err != nil {
			return err
		}
		role.Labels = r.labelsForShop(shop)
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"patch", "delete", "get"},
			},
		}
		return nil
	}); err != nil {
		return err
	}

	roleBinding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: mongoDatabaseRBName, Namespace: shop.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, roleBinding, func() error {
		if err := controllerutil.SetControllerReference(shop, roleBinding, r.Scheme); err != nil {
			return err
		}
		roleBinding.Labels = r.labelsForShop(shop)
		roleBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     mongoDatabaseRoleName,
		}
		roleBinding.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      mongoDatabaseSAName,
			Namespace: shop.Namespace,
		}}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (r *ShopReconciler) deleteMongoRBAC(ctx context.Context, shop *shopopsv1.Shop) error {
	for _, obj := range []client.Object{
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: mongoDatabaseRBName, Namespace: shop.Namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: mongoDatabaseRoleName, Namespace: shop.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: mongoDatabaseSAName, Namespace: shop.Namespace}},
	} {
		if err := r.Delete(ctx, obj); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
	}

	return nil
}

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
func (r *ShopReconciler) reconcileMigrationJob(ctx context.Context, shop *shopopsv1.Shop, from, to string) (bool, *metav1.Condition, error) {
	jobName := r.migrationJobName(shop, from, to)

	job := &batchv1.Job{}
	err := r.Get(ctx, namespacedName(shop.Namespace, jobName), job)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, nil, err
	}

	if err == nil {
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				failureCondition := metav1.Condition{
					Type:               "DatabaseReady",
					Status:             metav1.ConditionFalse,
					Reason:             "MigrationFailed",
					Message:            fmt.Sprintf("Migration job %s failed: %s", jobName, condition.Message),
					ObservedGeneration: shop.Generation,
				}
				return false, &failureCondition, nil
			}
		}
		if job.Status.Succeeded > 0 {
			return true, nil, nil
		}
		return false, nil, nil
	}

	backoffLimit := int32(3)
	ttlSecondsAfterFinished := int32(3600)
	jobLabels := r.migrationJobLabels(shop, from, to)
	newJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: shop.Namespace,
			Labels:    jobLabels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: jobLabels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:    "migration",
						Image:   shop.Spec.ApiImage,
						Command: []string{"node", "dist/migration/migrate.js", "--from", from, "--to", to},
						Env: []corev1.EnvVar{
							{
								Name: "POSTGRES_URL",
								ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: r.appSecretName(shop)},
									Key:                  "postgres-url",
								}},
							},
							{
								Name: "MONGO_URL",
								ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: r.appSecretName(shop)},
									Key:                  "mongo-url",
								}},
							},
						},
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(shop, newJob, r.Scheme); err != nil {
		return false, nil, err
	}

	return false, nil, r.Create(ctx, newJob)
}

func (r *ShopReconciler) reconcileDeployment(ctx context.Context, shop *shopopsv1.Shop, cfg deploymentConfig) (*appsv1.Deployment, error) {
	replicas := r.desiredReplicas(shop)
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: cfg.name, Namespace: shop.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		if err := controllerutil.SetControllerReference(shop, deployment, r.Scheme); err != nil {
			return err
		}

		deployment.Labels = cfg.labels
		deployment.Spec.Replicas = &replicas
		deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: cfg.selectors}
		deployment.Spec.Template.ObjectMeta.Labels = cfg.selectors
		deployment.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            cfg.name,
			Image:           cfg.image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports: []corev1.ContainerPort{{
				Name:          "http",
				ContainerPort: cfg.port,
			}},
			Env: cfg.env,
		}}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return deployment, nil
}

func (r *ShopReconciler) reconcileService(ctx context.Context, shop *shopopsv1.Shop, name string, targetPort int32, labels map[string]string, selectors map[string]string) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: shop.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := controllerutil.SetControllerReference(shop, service, r.Scheme); err != nil {
			return err
		}

		service.Labels = labels
		service.Spec.Selector = selectors
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       servicePort,
			TargetPort: intstr.FromInt32(targetPort),
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
					Paths: []networkingv1.HTTPIngressPath{
						{
							Path:     "/api",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: r.apiServiceName(shop),
									Port: networkingv1.ServiceBackendPort{Number: servicePort},
								},
							},
						},
						{
							Path:     "/auth",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: r.apiServiceName(shop),
									Port: networkingv1.ServiceBackendPort{Number: servicePort},
								},
							},
						},
						{
							Path:     "/",
							PathType: &pathType,
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: r.webServiceName(shop),
									Port: networkingv1.ServiceBackendPort{Number: servicePort},
								},
							},
						},
					},
				},
			},
		}}

		return nil
	})

	return err
}

func (r *ShopReconciler) reconcileStatus(ctx context.Context, shop *shopopsv1.Shop, deployment *appsv1.Deployment, databaseCondition metav1.Condition, activeDB shopopsv1.DatabaseType) error {
	latest := &shopopsv1.Shop{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(shop), latest); err != nil {
		return err
	}

	originalStatus := latest.Status.DeepCopy()
	desiredReplicas := r.desiredReplicas(shop)
	availableReplicas := int32(0)

	readyCondition := metav1.Condition{
		Type:               "Available",
		Status:             metav1.ConditionFalse,
		Reason:             "Progressing",
		Message:            fmt.Sprintf("Waiting for %d replicas to become available", desiredReplicas),
		ObservedGeneration: latest.Generation,
	}

	if deployment == nil {
		readyCondition.Reason = "WaitingForDatabase"
		readyCondition.Message = "Waiting for database to become ready before reconciling Deployments"
	} else {
		availableReplicas = deployment.Status.AvailableReplicas
		if availableReplicas >= desiredReplicas {
			readyCondition.Status = metav1.ConditionTrue
			readyCondition.Reason = "Available"
			readyCondition.Message = "Deployment is available"
		}
	}

	if activeDB != "" {
		latest.Status.ActiveDatabase = activeDB
	}

	latest.Status.Replicas = availableReplicas
	latest.Status.URL = r.shopURL(shop)

	apimeta.SetStatusCondition(&latest.Status.Conditions, readyCondition)
	apimeta.SetStatusCondition(&latest.Status.Conditions, databaseCondition)

	switch {
	case readyCondition.Status == metav1.ConditionTrue && databaseCondition.Status == metav1.ConditionTrue:
		latest.Status.Phase = "Ready"
	case databaseCondition.Status == metav1.ConditionFalse && (databaseCondition.Reason == "DatabaseCRDMissing" || databaseCondition.Reason == "UnsupportedDatabaseType" || databaseCondition.Reason == "MigrationFailed"):
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

func (r *ShopReconciler) desiredMongoDatabase(shop *shopopsv1.Shop) *unstructured.Unstructured {
	mongo := &unstructured.Unstructured{}
	mongo.SetGroupVersionKind(mongoDBCommunityGVK)
	mongo.SetName(r.databaseClusterName(shop))
	mongo.SetNamespace(shop.Namespace)
	return mongo
}

func (r *ShopReconciler) desiredReplicas(shop *shopopsv1.Shop) int32 {
	if shop.Spec.Availability == shopopsv1.AvailabilityHigh {
		return 3
	}

	return 2
}

func (r *ShopReconciler) apiEnvVars(shop *shopopsv1.Shop) []corev1.EnvVar {
	secretRef := func(key string) *corev1.EnvVarSource {
		return &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: r.appSecretName(shop)},
				Key:                  key,
			},
		}
	}

	return []corev1.EnvVar{
		{Name: "PORT", Value: fmt.Sprintf("%d", apiContainerPort)},
		{Name: "CORS_ORIGIN", Value: fmt.Sprintf("http://%s", shop.Spec.Host)},
		{Name: "DATABASE_URL", ValueFrom: secretRef("database-url")},
		{Name: "ADMIN_EMAIL", ValueFrom: secretRef("admin-email")},
		{Name: "ADMIN_PASSWORD", ValueFrom: secretRef("admin-password")},
		{Name: "JWT_SECRET", ValueFrom: secretRef("jwt-secret")},
		{Name: "WALLET_ADDRESS", Value: r.walletAddress(shop)},
		{Name: "SHOP_NAME", Value: r.displayName(shop)},
		{Name: "DB_MODE", Value: string(shop.Spec.Database.Type)},
	}
}

func (r *ShopReconciler) labelsForShop(shop *shopopsv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "shop",
		"app.kubernetes.io/instance":   shop.Name,
		"app.kubernetes.io/managed-by": "shop-operator",
		"shopops.shopops.dc.com/shop":  shop.Name,
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

func (r *ShopReconciler) reconcileMongoDatabaseURL(ctx context.Context, shop *shopopsv1.Shop) (string, error) {
	connectionString, ready, err := r.mongoConnectionString(ctx, shop)
	if err != nil {
		return "", err
	}
	if !ready {
		return "", nil
	}
	return connectionString, nil
}

func (r *ShopReconciler) databaseURLFor(ctx context.Context, shop *shopopsv1.Shop, dbType shopopsv1.DatabaseType) (string, error) {
	switch dbType {
	case shopopsv1.DatabaseStandard:
		return r.postgresDatabaseURL(ctx, shop)
	case shopopsv1.DatabaseLight:
		return r.reconcileMongoDatabaseURL(ctx, shop)
	default:
		return "", nil
	}
}

func (r *ShopReconciler) migrationURLsReady(ctx context.Context, shop *shopopsv1.Shop, from, to string) (bool, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, namespacedName(shop.Namespace, r.appSecretName(shop)), secret); err != nil {
		return false, err
	}

	requiredKeys := map[string]string{
		"postgres": "postgres-url",
		"mongo":    "mongo-url",
	}

	for _, driver := range []string{from, to} {
		key, ok := requiredKeys[driver]
		if !ok {
			return false, fmt.Errorf("unsupported migration driver %q", driver)
		}
		if len(secret.Data[key]) == 0 {
			return false, nil
		}
	}

	return true, nil
}

func (r *ShopReconciler) migrationJobName(shop *shopopsv1.Shop, from, to string) string {
	return fmt.Sprintf("%s-migrate-%s-to-%s-gen-%d", shop.Name, from, to, shop.Generation)
}

func (r *ShopReconciler) migrationJobLabels(shop *shopopsv1.Shop, from, to string) map[string]string {
	labels := r.labelsForShop(shop)
	labels["shopops.shopops.dc.com/migration-from"] = from
	labels["shopops.shopops.dc.com/migration-to"] = to
	labels["shopops.shopops.dc.com/migration-generation"] = fmt.Sprintf("%d", shop.Generation)
	return labels
}

func (r *ShopReconciler) ingressName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-ingress", shop.Name)
}

func (r *ShopReconciler) databaseClusterName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-db", shop.Name)
}

func (r *ShopReconciler) databaseReadWriteServiceName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-rw", r.databaseClusterName(shop))
}

func (r *ShopReconciler) mongoConnectionSecretName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-mongo-connection", shop.Name)
}

func (r *ShopReconciler) mongoDefaultConnectionSecretName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-%s-%s", r.databaseClusterName(shop), mongoAuthDB, mongoUsername)
}

func (r *ShopReconciler) mongoSCRAMSecretName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-mongo-scram", shop.Name)
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

func (r *ShopReconciler) apiDeploymentName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-api", shop.Name)
}

func (r *ShopReconciler) webDeploymentName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-web", shop.Name)
}

func (r *ShopReconciler) apiServiceName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-api", shop.Name)
}

func (r *ShopReconciler) webServiceName(shop *shopopsv1.Shop) string {
	return fmt.Sprintf("%s-web", shop.Name)
}

func (r *ShopReconciler) labelsForApi(shop *shopopsv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":           "shop-api",
		"app.kubernetes.io/instance":       shop.Name,
		"app.kubernetes.io/managed-by":     "shop-operator",
		"shopops.shopops.dc.com/shop":      shop.Name,
		"shopops.shopops.dc.com/component": "api",
	}
}

func (r *ShopReconciler) selectorLabelsForApi(shop *shopopsv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/instance":       shop.Name,
		"shopops.shopops.dc.com/shop":      shop.Name,
		"shopops.shopops.dc.com/component": "api",
	}
}

func (r *ShopReconciler) labelsForWeb(shop *shopopsv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":           "shop-web",
		"app.kubernetes.io/instance":       shop.Name,
		"app.kubernetes.io/managed-by":     "shop-operator",
		"shopops.shopops.dc.com/shop":      shop.Name,
		"shopops.shopops.dc.com/component": "web",
	}
}

func (r *ShopReconciler) selectorLabelsForWeb(shop *shopopsv1.Shop) map[string]string {
	return map[string]string{
		"app.kubernetes.io/instance":       shop.Name,
		"shopops.shopops.dc.com/shop":      shop.Name,
		"shopops.shopops.dc.com/component": "web",
	}
}
