// Postgres arrays in their text form, for the array tag: database/sql has no
// array type, so a slice goes to the database as an array literal and comes
// back as one.

package barm

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// appendArray appends v, a slice or a pointer to one, as an array literal.
func appendArray(b []byte, v reflect.Value) ([]byte, error) {
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	b = append(b, '{')
	for i := range v.Len() {
		if i > 0 {
			b = append(b, ',')
		}
		var err error
		b, err = appendElem(b, v.Index(i))
		if err != nil {
			return nil, err
		}
	}
	return append(b, '}'), nil
}

func appendElem(b []byte, v reflect.Value) ([]byte, error) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return append(b, "NULL"...), nil
		}
		v = v.Elem()
	}
	if vr, ok := reflect.TypeAssert[driver.Valuer](v); ok {
		dv, err := vr.Value()
		if err != nil {
			return nil, err
		}
		if dv == nil {
			return append(b, "NULL"...), nil
		}
		v = reflect.ValueOf(dv)
	}
	if v.Type() == timeType {
		return appendQuoted(b, v.Interface().(time.Time).Format(time.RFC3339Nano)), nil //nolint:forcetypeassert // checked just above
	}
	switch v.Kind() {
	case reflect.String:
		return appendQuoted(b, v.String()), nil
	case reflect.Bool:
		if v.Bool() {
			return append(b, 't'), nil
		}
		return append(b, 'f'), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(b, v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(b, v.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		switch {
		case math.IsInf(f, 1):
			return append(b, "Infinity"...), nil
		case math.IsInf(f, -1):
			return append(b, "-Infinity"...), nil
		}
		return strconv.AppendFloat(b, f, 'g', -1, v.Type().Bits()), nil
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return appendQuoted(b, `\x`+hex.EncodeToString(v.Bytes())), nil // bytea
		}
		return appendArray(b, v)
	}
	return nil, fmt.Errorf("barm: %s cannot be an array element", v.Type())
}

// appendQuoted appends s as a quoted element, which is right for any text,
// including the empty string and the word NULL.
func appendQuoted(b []byte, s string) []byte {
	b = append(b, '"')
	for i := range len(s) {
		if s[i] == '"' || s[i] == '\\' {
			b = append(b, '\\')
		}
		b = append(b, s[i])
	}
	return append(b, '"')
}

// decodeArray reads an array literal into dst, a slice or a pointer to one.
func decodeArray(s string, dst reflect.Value) error {
	if strings.HasPrefix(s, "[") { // explicit bounds, "[0:1]={1,2}"
		_, s, _ = strings.Cut(s, "=")
	}
	p := arrayParser{s: s}
	err := p.array(dst)
	p.space()
	if err == nil && p.i != len(p.s) {
		err = p.errorf("trailing text")
	}
	return err
}

type arrayParser struct {
	s string
	i int
}

func (p *arrayParser) errorf(what string) error {
	return fmt.Errorf("barm: malformed array %q: %s at %d", p.s, what, p.i)
}

func (p *arrayParser) space() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n' || p.s[p.i] == '\r') {
		p.i++
	}
}

func (p *arrayParser) array(dst reflect.Value) error {
	if dst.Kind() == reflect.Pointer {
		dst.Set(reflect.New(dst.Type().Elem()))
		dst = dst.Elem()
	}
	if dst.Kind() != reflect.Slice {
		return fmt.Errorf("barm: cannot read an array into %s", dst.Type())
	}
	p.space()
	if p.i == len(p.s) || p.s[p.i] != '{' {
		return p.errorf("expected {")
	}
	p.i++
	out := reflect.MakeSlice(dst.Type(), 0, strings.Count(p.s[p.i:], ",")+1)
	p.space()
	if p.i < len(p.s) && p.s[p.i] == '}' {
		p.i++
		dst.Set(out)
		return nil
	}
	for {
		out = reflect.Append(out, reflect.Zero(dst.Type().Elem()))
		elem := out.Index(out.Len() - 1)
		p.space()
		if p.i < len(p.s) && p.s[p.i] == '{' {
			err := p.array(elem)
			if err != nil {
				return err
			}
		} else {
			text, null, err := p.token()
			if err != nil {
				return err
			}
			err = setElem(elem, text, null)
			if err != nil {
				return err
			}
		}
		p.space()
		if p.i == len(p.s) {
			return p.errorf("unterminated")
		}
		c := p.s[p.i]
		p.i++
		if c == '}' {
			dst.Set(out)
			return nil
		}
		if c != ',' {
			return p.errorf("expected , or }")
		}
	}
}

// token reads one element: quoted, with backslash escapes, or bare up to the
// next delimiter, where the word NULL is a NULL.
func (p *arrayParser) token() (string, bool, error) {
	quoted := p.i < len(p.s) && p.s[p.i] == '"'
	if quoted {
		p.i++
	}
	var sb strings.Builder
	start, plain := p.i, true // plain: no escapes, so the text is a substring
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch {
		case c == '\\':
			if plain {
				sb.WriteString(p.s[start:p.i])
				plain = false
			}
			p.i++
			if p.i == len(p.s) {
				return "", false, p.errorf("unterminated escape")
			}
			sb.WriteByte(p.s[p.i])
			p.i++
			continue
		case quoted && c == '"':
			text := p.s[start:p.i]
			p.i++
			if !plain {
				text = sb.String()
			}
			return text, false, nil
		case !quoted && (c == ',' || c == '}'):
			text := p.s[start:p.i]
			if !plain {
				text = sb.String()
			}
			text = strings.TrimRight(text, " \t\n\r")
			return text, strings.EqualFold(text, "NULL"), nil
		}
		if !plain {
			sb.WriteByte(c)
		}
		p.i++
	}
	return "", false, p.errorf("unterminated")
}

var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999Z07:00:00",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999",
	time.RFC3339Nano,
	time.DateOnly,
}

// setElem sets an element from its text. NULL is a nil pointer, or the zero
// value of anything else.
func setElem(v reflect.Value, s string, null bool) error {
	if null {
		v.SetZero()
		return nil
	}
	if v.Kind() == reflect.Pointer {
		v.Set(reflect.New(v.Type().Elem()))
		v = v.Elem()
	}
	if sc, ok := reflect.TypeAssert[sql.Scanner](v.Addr()); ok {
		return sc.Scan(s)
	}
	if v.Type() == timeType {
		for _, layout := range timeLayouts {
			t, err := time.Parse(layout, s)
			if err == nil {
				v.Set(reflect.ValueOf(t))
				return nil
			}
		}
		return fmt.Errorf("barm: cannot read %q as a time", s)
	}
	var err error
	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Bool:
		var b bool
		b, err = strconv.ParseBool(s)
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var n int64
		n, err = strconv.ParseInt(s, 10, v.Type().Bits())
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var n uint64
		n, err = strconv.ParseUint(s, 10, v.Type().Bits())
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		var f float64
		f, err = strconv.ParseFloat(s, v.Type().Bits())
		v.SetFloat(f)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 || !strings.HasPrefix(s, `\x`) {
			return fmt.Errorf("barm: cannot read %q into %s", s, v.Type())
		}
		var b []byte
		b, err = hex.DecodeString(s[2:])
		v.SetBytes(b)
	default:
		return fmt.Errorf("barm: %s cannot be an array element", v.Type())
	}
	if err != nil {
		return fmt.Errorf("barm: cannot read %q into %s: %w", s, v.Type(), err)
	}
	return nil
}
