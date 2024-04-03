package state

import (
	"k8s.io/utils/cpuset"
)

type RtState struct {
	State
	containerToUtil map[string]float64
	cpuToUtil       map[int]float64
}

func NewRtState(s State) *RtState {
	rts := &RtState{
		State:           s,
		containerToUtil: make(map[string]float64),
	}

	rts.cpuToUtil = make(map[int]float64, s.GetDefaultCPUSet().Size())
	for _, cpu := range s.GetDefaultCPUSet().UnsortedList() {
		rts.cpuToUtil[cpu] = 0
	}

	return rts
}

func (s RtState) GetRtCPUSetAndUtilOfContainer(podUID string, containerName string) (cpuset.CPUSet, float64, bool) {
	cpuSet, ok := s.GetCPUSet(podUID, containerName)
	if !ok {
		return cpuset.CPUSet{}, 0, false
	}

	util, ok := s.containerToUtil[containerName]
	if !ok {
		return cpuset.CPUSet{}, 0, false
	}

	return cpuSet, util, true
}

func (s *RtState) SetRtCPUSetAndUtilOfContainer(podUID string, containerName string, set cpuset.CPUSet, util float64) {

	oldUtil, ok := s.containerToUtil[containerName]
	if ok {
		// container was already set, we must first clean
		oldSet, ok := s.GetCPUSet(podUID, containerName)
		if !ok {
			panic("found utilization but not cpuset")
		}
		for _, cpu := range oldSet.UnsortedList() {
			s.cpuToUtil[cpu] -= oldUtil
		}
	}

	s.SetCPUSet(podUID, containerName, set)
	s.containerToUtil[containerName] = util

	for _, cpu := range set.UnsortedList() {
		s.cpuToUtil[cpu] += util
	}
}

func (s *RtState) Delete(podUID string, containerName string) {

	cpuSet, ok := s.GetCPUSet(podUID, containerName)
	if !ok {
		panic("manage this error")
	}

	cpuSet, containerUtil, ok := s.GetRtCPUSetAndUtilOfContainer(podUID, containerName)
	if !ok {
		// it wasn't assigned using SetRt
		s.State.Delete(podUID, containerName)
		return
	}

	for _, cpu := range cpuSet.UnsortedList() {
		s.cpuToUtil[cpu] -= containerUtil
		if s.cpuToUtil[cpu] < 0 {
			s.cpuToUtil[cpu] = 0
		}
	}
	delete(s.containerToUtil, containerName)

	s.State.Delete(podUID, containerName)
}

func (s *RtState) CpuToUtilMap() map[int]float64 {
	cpuToUtilMap := make(map[int]float64, len(s.cpuToUtil))
	for key, v := range s.cpuToUtil {
		cpuToUtilMap[key] = v
	}
	return cpuToUtilMap
}

func (s *RtState) SetDefaultCPUSet(set cpuset.CPUSet) {
	s.State.SetDefaultCPUSet(set)

	s.cpuToUtil = make(map[int]float64, set.Size())
	for _, cpu := range set.UnsortedList() {
		s.cpuToUtil[cpu] = 0
	}
}
