package cpumanager

import (
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/utils/cpuset"
)

// PolicyRealTime is the name of the real-time policy
const PolicyRealTime policyName = "real-time"

type RtState interface {
	state.State
	GetRtCPUSetAndUtilOfContainer(podID string, containerName string) (cpuset.CPUSet, float64, bool)
	SetRtCPUSetAndUtilOfContainer(podID string, containerName string, set cpuset.CPUSet, util float64)
	CpuToUtilMap() map[int]float64
}

type realTimePolicy struct {
	topology *topology.CPUTopology
	// allocable utilization
	allocableRtUtil float64
	// number of reserved cpus
	numReservedCpus int
	// unassignable cpus
	reservedCpus cpuset.CPUSet
}

// Ensure realTimePolicy implements Policy interface
var _ Policy = &realTimePolicy{}

func NewRealTimePolicy(topology *topology.CPUTopology, numReservedCPUs int, reservedCPUs cpuset.CPUSet, allocableRtUtil float64) (Policy, error) {

	policy := &realTimePolicy{
		topology:        topology,
		numReservedCpus: numReservedCPUs,
		reservedCpus:    reservedCPUs,
		allocableRtUtil: allocableRtUtil,
	}

	allCPUs := topology.CPUDetails.CPUs()
	var reserved cpuset.CPUSet
	if reservedCPUs.Size() > 0 {
		reserved = reservedCPUs
	} else {
		// takeByTopology allocates CPUs associated with low-numbered cores from
		// allCPUs.
		//
		// For example: Given a system with 8 CPUs available and HT enabled,
		// if numReservedCPUs=2, then reserved={0,4}
		reserved, _ = policy.takeByTopology(allCPUs, numReservedCPUs)
	}

	if reserved.Size() != numReservedCPUs {
		err := fmt.Errorf("[cpumanager] unable to reserve the required amount of CPUs (size of %s did not equal %d)", reserved, numReservedCPUs)
		return nil, err
	}

	return policy, nil
}

func (p *realTimePolicy) Name() string {
	return string(PolicyRealTime)
}

func (p *realTimePolicy) Start(s state.State) error {
	if err := p.validateState(s); err != nil {
		klog.ErrorS(err, "Real-time policy invalid state, please drain node and remove policy state file")
		return err
	}
	return nil
}

func (p *realTimePolicy) validateState(s state.State) error {
	tmpAssignments := s.GetCPUAssignments()

	// Default cpuset cannot be empty when assignments exist

	// state is empty initialize
	allCPUs := p.topology.CPUDetails.CPUs()
	s.SetDefaultCPUSet(allCPUs)

	// 2. Check if state for real-time policy is consistent
	for pod := range tmpAssignments {
		for container, _ := range tmpAssignments[pod] {
			s.Delete(pod, container)
		}
	}
	return nil
}

// GetAllocatableCPUs returns the total set of CPUs available for allocation.
func (p *realTimePolicy) GetAllocatableCPUs(s state.State) cpuset.CPUSet {
	return p.topology.CPUDetails.CPUs()
}

// GetAvailableCPUs returns the set of unassigned CPUs minus the reserved set.
func (p *realTimePolicy) GetAvailableCPUs(s state.State) cpuset.CPUSet {
	return s.GetDefaultCPUSet()
}

func (p *realTimePolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) (rerr error) {
	rtState := s.(RtState)

	reqPeriod, reqRuntime, reqCpus := rtRequests(container)
	reqUtil := float64(0)
	if reqPeriod != 0 {
		reqUtil = float64(reqRuntime) / float64(reqPeriod)
	}

	if reqUtil == 0 {
		// no cpu management
		return nil
	}

	if _, _, ok := rtState.GetRtCPUSetAndUtilOfContainer(string(pod.UID), container.Name); ok {
		klog.Infof("[cpumanager] real-time policy: container already assigned to cpus, skipping (container: %s, pod id: %s)", container.Name, pod.UID)
		return nil
	}

	cpus := p.worstFit(rtState.CpuToUtilMap(), reqUtil, reqCpus)
	if int64(len(cpus)) < reqCpus {
		err := fmt.Errorf("container %s doesn't fit", container.Name)
		klog.Errorf("[cpumanager] unable to allocate %d CPUs (container name: %s, error: %v)", reqCpus, container.Name, err)
		return err
	}
	fittingCpusSet := cpuset.New(cpus...)

	rtState.SetRtCPUSetAndUtilOfContainer(string(pod.UID), container.Name, fittingCpusSet, reqUtil)

	return nil
}

func (p *realTimePolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	klog.Infof("[cpumanager] real-time policy: RemoveContainer (container id: %s)", containerName)
	rtState := s.(RtState)

	_, _, ok := rtState.GetRtCPUSetAndUtilOfContainer(podUID, containerName)
	if !ok {
		// container not assigned by real-time policy
		return nil
	}

	s.Delete(podUID, containerName)

	return nil
}

func (p *realTimePolicy) allocateCPUs(s state.State, numCPUs int, numaAffinity bitmask.BitMask, reusableCPUs cpuset.CPUSet) (cpuset.CPUSet, error) {
	klog.InfoS("AllocateCPUs", "numCPUs", numCPUs, "socket", numaAffinity)

	allocatableCPUs := p.GetAvailableCPUs(s).Union(reusableCPUs)

	// If there are aligned CPUs in numaAffinity, attempt to take those first.
	result := cpuset.New()
	if numaAffinity != nil {
		alignedCPUs := p.getAlignedCPUs(numaAffinity, allocatableCPUs)

		numAlignedToAlloc := alignedCPUs.Size()
		if numCPUs < numAlignedToAlloc {
			numAlignedToAlloc = numCPUs
		}

		alignedCPUs, err := p.takeByTopology(alignedCPUs, numAlignedToAlloc)
		if err != nil {
			return cpuset.New(), err
		}

		result = result.Union(alignedCPUs)
	}

	// Get any remaining CPUs from what's leftover after attempting to grab aligned ones.
	remainingCPUs, err := p.takeByTopology(allocatableCPUs.Difference(result), numCPUs-result.Size())
	if err != nil {
		return cpuset.New(), err
	}
	result = result.Union(remainingCPUs)

	// Remove allocated CPUs from the shared CPUSet.
	s.SetDefaultCPUSet(s.GetDefaultCPUSet().Difference(result))

	klog.InfoS("AllocateCPUs", "result", result)
	return result, nil
}

func (p *realTimePolicy) guaranteedCPUs(pod *v1.Pod, container *v1.Container) int {
	if v1qos.GetPodQOS(pod) != v1.PodQOSGuaranteed {
		return 0
	}
	cpuQuantity := container.Resources.Requests[v1.ResourceCPU]
	// In-place pod resize feature makes Container.Resources field mutable for CPU & memory.
	// AllocatedResources holds the value of Container.Resources.Requests when the pod was admitted.
	// We should return this value because this is what kubelet agreed to allocate for the container
	// and the value configured with runtime.
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		if cs, ok := podutil.GetContainerStatus(pod.Status.ContainerStatuses, container.Name); ok {
			cpuQuantity = cs.AllocatedResources[v1.ResourceCPU]
		}
	}
	if cpuQuantity.Value()*1000 != cpuQuantity.MilliValue() {
		return 0
	}
	// Safe downcast to do for all systems with < 2.1 billion CPUs.
	// Per the language spec, `int` is guaranteed to be at least 32 bits wide.
	// https://golang.org/ref/spec#Numeric_types
	return int(cpuQuantity.Value())
}

func (p *realTimePolicy) podGuaranteedCPUs(pod *v1.Pod) int {
	// The maximum of requested CPUs by init containers.
	requestedByInitContainers := 0
	requestedByRestartableInitContainers := 0
	for _, container := range pod.Spec.InitContainers {
		if _, ok := container.Resources.Requests[v1.ResourceCPU]; !ok {
			continue
		}
		requestedCPU := p.guaranteedCPUs(pod, &container)
		// See https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/753-sidecar-containers#resources-calculation-for-scheduling-and-pod-admission
		// for the detail.
		if types.IsRestartableInitContainer(&container) {
			requestedByRestartableInitContainers += requestedCPU
		} else if requestedByRestartableInitContainers+requestedCPU > requestedByInitContainers {
			requestedByInitContainers = requestedByRestartableInitContainers + requestedCPU
		}
	}

	// The sum of requested CPUs by app containers.
	requestedByAppContainers := 0
	for _, container := range pod.Spec.Containers {
		if _, ok := container.Resources.Requests[v1.ResourceCPU]; !ok {
			continue
		}
		requestedByAppContainers += p.guaranteedCPUs(pod, &container)
	}

	requestedByLongRunningContainers := requestedByAppContainers + requestedByRestartableInitContainers
	if requestedByInitContainers > requestedByLongRunningContainers {
		return requestedByInitContainers
	}
	return requestedByLongRunningContainers
}

func (p *realTimePolicy) takeByTopology(availableCPUs cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	return takeByTopologyNUMAPacked(p.topology, availableCPUs, numCPUs)
}

func (p *realTimePolicy) GetTopologyHints(s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {
	panic("implement me")
}

func (p *realTimePolicy) GetPodTopologyHints(s state.State, pod *v1.Pod) map[string][]topologymanager.TopologyHint {
	panic("implement me")
}

// generateCPUTopologyHints generates a set of TopologyHints given the set of
// available CPUs and the number of CPUs being requested.
//
// It follows the convention of marking all hints that have the same number of
// bits set as the narrowest matching NUMANodeAffinity with 'Preferred: true', and
// marking all others with 'Preferred: false'.

// isHintSocketAligned function return true if numa nodes in hint are socket aligned.
func (p *realTimePolicy) isHintSocketAligned(hint topologymanager.TopologyHint, minAffinitySize int) bool {
	numaNodesBitMask := hint.NUMANodeAffinity.GetBits()
	numaNodesPerSocket := p.topology.NumNUMANodes / p.topology.NumSockets
	if numaNodesPerSocket == 0 {
		return false
	}
	// minSockets refers to minimum number of socket required to satify allocation.
	// A hint is considered socket aligned if sockets across which numa nodes span is equal to minSockets
	minSockets := (minAffinitySize + numaNodesPerSocket - 1) / numaNodesPerSocket
	return p.topology.CPUDetails.SocketsInNUMANodes(numaNodesBitMask...).Size() == minSockets
}

// getAlignedCPUs return set of aligned CPUs based on numa affinity mask and configured policy options.
func (p *realTimePolicy) getAlignedCPUs(numaAffinity bitmask.BitMask, allocatableCPUs cpuset.CPUSet) cpuset.CPUSet {
	alignedCPUs := cpuset.New()
	numaBits := numaAffinity.GetBits()

	for _, numaNodeID := range numaBits {
		alignedCPUs = alignedCPUs.Union(allocatableCPUs.Intersection(p.topology.CPUDetails.CPUsInNUMANodes(numaNodeID)))
	}

	return alignedCPUs
}

func (p *realTimePolicy) worstFit(cpuToUtil map[int]float64, reqUtil float64, reqCpus int64) []int {
	type scoredCpu struct {
		cpu   int
		score float64
	}

	var scoredCpus []scoredCpu
	for cpu, util := range cpuToUtil {
		score := p.allocableRtUtil - util - reqUtil
		if score > 0 {
			scoredCpus = append(scoredCpus, scoredCpu{
				cpu:   cpu,
				score: score,
			})
		}
	}

	if int64(len(scoredCpus)) < reqCpus {
		return nil
	}

	sort.SliceStable(scoredCpus, func(i, j int) bool {
		if scoredCpus[i].score > scoredCpus[j].score {
			return true
		}
		return false
	})

	var fittingCpus []int
	for i := int64(0); i < reqCpus; i++ {
		fittingCpus = append(fittingCpus, scoredCpus[i].cpu)
	}

	return fittingCpus
}

func (p *realTimePolicy) bestFit(cpuToUtil map[int]float64, reqUtil float64, reqCpus int64) []int {
	type scoredCpu struct {
		cpu   int
		score float64
	}

	var scoredCpus []scoredCpu
	for cpu, util := range cpuToUtil {
		score := p.allocableRtUtil - util - reqUtil
		if score > 0 {
			scoredCpus = append(scoredCpus, scoredCpu{
				cpu:   cpu,
				score: score,
			})
		}
	}

	if int64(len(scoredCpus)) < reqCpus {
		return nil
	}

	sort.SliceStable(scoredCpus, func(i, j int) bool {
		if scoredCpus[i].score < scoredCpus[j].score {
			return true
		}
		return false
	})

	var fittingCpus []int
	for i := int64(0); i < reqCpus; i++ {
		fittingCpus = append(fittingCpus, scoredCpus[i].cpu)
	}

	return fittingCpus
}

func rtRequests(container *v1.Container) (int64, int64, int64) {
	return container.Resources.Requests.CpuRtPeriod().Value(),
		container.Resources.Requests.CpuRtRuntime().Value(),
		container.Resources.Requests.CpuRt().Value()
}

// assignableCPUs returns the set of unassigned CPUs minus the reserved set.
func (p *realTimePolicy) assignableCPUs(s state.State) cpuset.CPUSet {
	return s.GetDefaultCPUSet().Difference(p.reservedCpus)
}
