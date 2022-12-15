package s3fs

// Copyright (C) 2022 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// Package s3 implements a lightweight
// client of the AWS S3 API.
//
// The Reader type can be used to view
// S3 objects as an io.Reader or io.ReaderAt.

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// rangeReader produces an io.ReadCloser that reads
// bytes in the range from [off, off+width)
//
// It is the caller's responsibility to call Close()
// on the returned io.ReadCloser.
func (f *s3File) rangeReader(from, amt int64) (io.ReadCloser, error) {
	amt = amt + READAHEAD
	target := from + amt - 1
	if target >= f.info.Size() {
		target = f.info.Size() - 1
	}
	if from >= f.info.Size() {
		return nil, io.EOF
	}
	rq := &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.name),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", from, target)),
	}
	res, err := f.fs.s3.GetObjectWithContext(context.TODO(), rq)
	if err != nil {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, err
	}
	return res.Body, nil
}
