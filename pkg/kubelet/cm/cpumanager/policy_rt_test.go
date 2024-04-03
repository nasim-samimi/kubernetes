package cpumanager

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/utils/cpuset"
)

func Test_realTimePolicy_AddContainer(t *testing.T) {
	type fields struct {
		topology        *topology.CPUTopology
		allocableRtUtil float64
		numReservedCpus int
		reservedCpus    cpuset.CPUSet
	}
	type args struct {
		s           state.State
		pod         *v1.Pod
		container   *v1.Container
		containerID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "all Cpus unoccupied",
			fields: fields{
				topology:        topoDualSocketNoHT,
				allocableRtUtil: 0.95,
				numReservedCpus: 2,
				reservedCpus:    cpuset.New(),
			},
			args: args{
				s: state.NewRtState(&mockState{
					assignments:   make(state.ContainerCPUAssignments),
					defaultCPUSet: cpuset.CPUSet{},
				}),
				pod: &v1.Pod{
					TypeMeta:   metav1.TypeMeta{},
					ObjectMeta: metav1.ObjectMeta{},
					Spec:       v1.PodSpec{},
					Status:     v1.PodStatus{},
				},
				container: &v1.Container{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceRtPeriod:  *resource.NewQuantity(1000000, resource.DecimalSI),
							v1.ResourceRtRuntime: *resource.NewQuantity(100000, resource.DecimalSI),
							v1.ResourceRtCpu:     *resource.NewQuantity(2, resource.DecimalSI),
						},
					},
				},
				containerID: "test-rt-policy",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := NewRealTimePolicy(tt.fields.topology, tt.fields.numReservedCpus, tt.fields.reservedCpus, tt.fields.allocableRtUtil)
			p.Start(tt.args.s)
			if err := p.Allocate(tt.args.s, tt.args.pod, tt.args.container); (err != nil) != tt.wantErr {
				t.Errorf("AddContainer() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := p.RemoveContainer(tt.args.s, string(tt.args.pod.UID), tt.args.container.Name); err != nil {
				t.Errorf("RemoveContainer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
