/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package http

import (
	"net/http"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestHeaderToMap(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		expect func(t *testing.T, data interface{})
	}{
		{
			name: "normal conversion",
			header: http.Header{
				"foo": {"foo"},
				"bar": {"bar"},
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, map[string]string{
					"foo": "foo",
					"bar": "bar",
				})
			},
		},
		{
			name:   "header is empty",
			header: http.Header{},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, map[string]string{})
			},
		},
		{
			name: "header is a nested array",
			header: http.Header{
				"foo": {"foo1", "foo2"},
				"bar": {"bar"},
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, map[string]string{
					"foo": "foo1",
					"bar": "bar",
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := HeaderToMap(tc.header)
			tc.expect(t, data)
		})
	}
}

func TestMapToHeader(t *testing.T) {
	tests := []struct {
		name   string
		m      map[string]string
		expect func(t *testing.T, data interface{})
	}{
		{
			name: "normal conversion",
			m: map[string]string{
				"Foo": "foo",
				"Bar": "bar",
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, http.Header{
					"Foo": {"foo"},
					"Bar": {"bar"},
				})
			},
		},
		{
			name: "map is empty",
			m:    map[string]string{},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, http.Header{})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := MapToHeader(tc.m)
			tc.expect(t, data)
		})
	}
}

func TestPickHeader(t *testing.T) {
	tests := []struct {
		name         string
		header       http.Header
		key          string
		defaultValue string
		expect       func(t *testing.T, data string, header http.Header)
	}{
		{
			name: "Pick the existing key",
			header: http.Header{
				"Foo": {"foo"},
				"Bar": {"bar"},
			},
			key:          "Foo",
			defaultValue: "",
			expect: func(t *testing.T, data string, header http.Header) {
				assert := testifyassert.New(t)
				assert.Equal("foo", data)
				assert.Equal("", header.Get("Foo"))
			},
		},
		{
			name:         "Pick the non-existent key",
			header:       http.Header{},
			key:          "Foo",
			defaultValue: "bar",
			expect: func(t *testing.T, data string, header http.Header) {
				assert := testifyassert.New(t)
				assert.Equal(data, "bar")
				assert.Equal(header.Get("Foo"), "")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := PickHeader(tc.header, tc.key, tc.defaultValue)
			tc.expect(t, data, tc.header)
		})
	}
}
