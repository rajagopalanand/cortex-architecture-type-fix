package rulerqueryscheduler

import (
	encoding_binary "encoding/binary"
	"fmt"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"io"
	"math"
)

type HistogramAdapter histogram.FloatHistogram

type SpanAdapter histogram.Span

type SampleAdapter promql.Sample

func (m *SampleAdapter) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRulerscheduler
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Sample: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Sample: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field T", wireType)
			}
			m.T = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRulerscheduler
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.T |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field F", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			m.F = float64(math.Float64frombits(v))
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Metric", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRulerscheduler
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRulerscheduler
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthRulerscheduler
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Metric = append(m.Metric, labels.Label(cortexpb.LabelAdapter{}))
			labelAdapter := cortexpb.LabelAdapter(m.Metric[len(m.Metric)-1])
			if err := labelAdapter.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRulerscheduler(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRulerscheduler
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthRulerscheduler
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *SampleAdapter) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.T != 0 {
		n += 1 + sovRulerscheduler(uint64(m.T))
	}
	if m.F != 0 {
		n += 9
	}
	if len(m.Metric) > 0 {
		for _, e := range m.Metric {
			labelAdapter := cortexpb.LabelAdapter(e)
			l = labelAdapter.Size()
			n += 1 + l + sovRulerscheduler(uint64(l))
		}
	}
	return n
}

func (this *SampleAdapter) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*Sample)
	if !ok {
		that2, ok := that.(Sample)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.T != that1.T {
		return false
	}
	if this.F != that1.F {
		return false
	}
	if len(this.Metric) != len(that1.Metric) {
		return false
	}
	for i := range this.Metric {
		labelAdapter := cortexpb.LabelAdapter(this.Metric[i])
		if !labelAdapter.Equal(that1.Metric[i]) {
			return false
		}
	}
	return true
}

func (m *SampleAdapter) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SampleAdapter) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Metric) > 0 {
		for iNdEx := len(m.Metric) - 1; iNdEx >= 0; iNdEx-- {
			{
				labelAdapter := cortexpb.LabelAdapter(m.Metric[iNdEx])
				size := labelAdapter.Size()
				i -= size
				if _, err := labelAdapter.MarshalTo(dAtA[i:]); err != nil {
					return 0, err
				}
				i = encodeVarintRulerscheduler(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x22
		}
	}
	if m.F != 0 {
		i -= 8
		encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(m.F))))
		i--
		dAtA[i] = 0x11
	}
	if m.T != 0 {
		i = encodeVarintRulerscheduler(dAtA, i, uint64(m.T))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}
