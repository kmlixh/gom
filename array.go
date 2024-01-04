package gom

func ArrayOf(i ...interface{}) []interface{} {
	return i
}
func ArrayIntersect(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times, _ := m[v]
		if times == 1 {
			nn = append(nn, v)
			delete(m, v)
		}
	}
	return nn
}
func ArrayIntersect2(slice1, slice2 []string) ([]string, []string, []string) {
	m := make(map[string]int)
	intersect := make([]string, 0)
	left := make([]string, 0)
	right := make([]string, 0)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times, _ := m[v]
		if times == 1 {
			intersect = append(intersect, v)
			delete(m, v)
		} else {
			right = append(right, v)
		}
	}
	for key, _ := range m {
		left = append(left, key)
	}
	return left, intersect, right
}
