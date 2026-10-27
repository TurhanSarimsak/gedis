package store

// Match reports whether str matches a Redis-style glob pattern: * ? [abc]
// [^a-z] and \ escapes. Unlike path.Match, '*' also matches '/', because Redis
// keys are commonly namespaced like "user:42/profile".
func Match(pattern, str string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) > 1 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true
			}
			for i := 0; i <= len(str); i++ {
				if Match(pattern[1:], str[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(str) == 0 {
				return false
			}
		case '[':
			if len(str) == 0 {
				return false
			}
			end, matched := matchClass(pattern, str[0])
			if end < 0 { // unterminated class: treat '[' literally
				if str[0] != '[' {
					return false
				}
				break
			}
			if !matched {
				return false
			}
			pattern = pattern[end:]
		case '\\':
			if len(pattern) > 1 {
				pattern = pattern[1:]
			}
			fallthrough
		default:
			if len(str) == 0 || str[0] != pattern[0] {
				return false
			}
		}
		pattern, str = pattern[1:], str[1:]
	}
	return len(str) == 0
}

// matchClass matches c against the class starting at pattern[0] == '['. It
// returns the index of the closing ']' (or -1 if there is none).
func matchClass(pattern string, c byte) (end int, matched bool) {
	i := 1
	negate := i < len(pattern) && pattern[i] == '^'
	if negate {
		i++
	}
	for i < len(pattern) {
		switch {
		case pattern[i] == ']':
			return i, matched != negate
		case pattern[i] == '\\' && i+1 < len(pattern):
			matched = matched || pattern[i+1] == c
			i += 2
		case i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']':
			lo, hi := pattern[i], pattern[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			matched = matched || (c >= lo && c <= hi)
			i += 3
		default:
			matched = matched || pattern[i] == c
			i++
		}
	}
	return -1, false
}
