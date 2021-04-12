package resource

import (
	"fmt"
	"github.com/scylladb/scylla-operator/pkg/naming"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"
)

func TestGetIpFromService(t *testing.T) {
	clusterIp := "10.10.10.10"
	podIp := "20.20.20.20"
	labels := map[string]string{
		naming.IpLabel: podIp,
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod_service",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: clusterIp,
		},
	}

	ip, _ := GetIpFromService(service, false)
	if ip != clusterIp {
		t.Error("IP is not well retrieved from service ClusterIp spec")
	}

	_, err := GetIpFromService(service, true)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%s label not found on member service pod_service", naming.IpLabel)) {
		t.Errorf("No Error or bad error return while %s label is missing on the service: %v", naming.IpLabel, err)
	}

	service.ObjectMeta.Labels = labels
	ip, _ = GetIpFromService(service, true)
	if ip != podIp {
		t.Errorf("IP is not well retrieved from %s label", naming.IpLabel)
	}
}
