package k8sutil

import (
	"fmt"
	"path"
	"time"

	"github.com/coreos/etcd-operator/pkg/util/retryutil"
	"k8s.io/api/core/v1"
	v1beta1storage "k8s.io/api/storage/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	DataStorageClassPrefix = "etcd"
)

func CreateDataStorageClass(kubecli kubernetes.Interface, pvProvisioner string) error {
	// We need to get rid of prefix because naming doesn't support "/".
	name := DataStorageClassPrefix + "-" + path.Base(pvProvisioner)
	class := &v1beta1storage.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner: pvProvisioner,
	}
	_, err := kubecli.StorageV1beta1().StorageClasses().Create(class)
	return err
}

func CreateAndWaitDataPVC(kubecli kubernetes.Interface, clusterName, ns, pvProvisioner string, memberID uint64, volumeSizeInMB int) error {
	storageClassName := DataStorageClassPrefix + "-" + path.Base(pvProvisioner)
	name := makeDataPVCName(clusterName, memberID)
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"etcd_cluster": clusterName,
				"app":          "etcd",
			},
			Annotations: map[string]string{
				"volume.beta.kubernetes.io/storage-class": storageClassName,
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dMi", volumeSizeInMB)),
				},
			},
		},
	}
	_, err := kubecli.CoreV1().PersistentVolumeClaims(ns).Create(claim)
	if err != nil {
		if IsKubernetesResourceAlreadyExistError(err) {
			err = kubecli.CoreV1().PersistentVolumeClaims(ns).Delete(name, nil)
			if !IsKubernetesResourceNotFoundError(err) {
				return err
			}
			_, err = kubecli.CoreV1().PersistentVolumeClaims(ns).Create(claim)
			if IsKubernetesResourceAlreadyExistError(err) {
				return err
			}
		} else {
			return err
		}
	}

	// TODO: We set timeout to 60s here since PVC binding could take up to 60s for GCE/PD. See https://github.com/kubernetes/kubernetes/issues/40972 .
	//       Change the wait time once there are official p99 SLA.
	err = retryutil.Retry(4*time.Second, 15, func() (bool, error) {
		var err error
		claim, err = kubecli.CoreV1().PersistentVolumeClaims(ns).Get(name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if claim.Status.Phase != v1.ClaimBound {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		wErr := fmt.Errorf("fail to wait PVC (%s) '(%v)/Bound': %v", name, claim.Status.Phase, err)
		return wErr
	}

	return nil
}

func makeDataPVCName(clusterName string, memberID uint64) string {
	return fmt.Sprintf("%s-%d-data-pvc", clusterName, memberID)
}
