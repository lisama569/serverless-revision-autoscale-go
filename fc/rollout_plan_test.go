package fc

import "testing"

func TestPlanStepWeightsShape(t *testing.T) {
	start := map[RevisionID]int{"a": 1}
	target := map[RevisionID]int{"a": 1, "b": 1}
	steps := planStepWeights(start, target, 4)
	if len(steps) != 5 {
		t.Fatalf("want 5 weight tables, got %d", len(steps))
	}
	if steps[0]["a"] != 1 || steps[0]["b"] != 0 {
		t.Fatalf("step 0 must equal start: %v", steps[0])
	}
	if steps[4]["a"] != 1 || steps[4]["b"] != 1 {
		t.Fatalf("final step must equal target exactly: %v", steps[4])
	}
	// b 的占比单调不减（跨分母比较分数：b1/total1 <= b2/total2）；每步总和为正。
	prevNum, prevDen := 0, 1
	for i, w := range steps {
		if sumWeights(w) <= 0 {
			t.Fatalf("step %d not self-consistent: %v", i, w)
		}
		num, den := w["b"], sumWeights(w)
		if num*prevDen < prevNum*den {
			t.Fatalf("target ratio decreased at step %d: %v", i, w)
		}
		prevNum, prevDen = num, den
	}

	// 相同入参必须生成相同序列。
	again := planStepWeights(start, target, 4)
	for i := range steps {
		if !weightsEqual(steps[i], again[i]) {
			t.Fatalf("non-deterministic step %d: %v vs %v", i, steps[i], again[i])
		}
	}
}

func TestPlanStepWeightsSingleStep(t *testing.T) {
	start := map[RevisionID]int{"a": 3}
	target := map[RevisionID]int{"b": 2}
	steps := planStepWeights(start, target, 1)
	if len(steps) != 2 {
		t.Fatalf("want 2 tables, got %d", len(steps))
	}
	if !weightsEqual(steps[0], start) || !weightsEqual(steps[1], target) {
		t.Fatalf("single-step plan must be start then target only: %v", steps)
	}
}
