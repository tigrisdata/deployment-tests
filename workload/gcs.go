// GCS supports the S3 API but has specific requirements around request signing.
//
// This package provides middleware to temporarily remove problematic headers before
// signing, then restore them after signing is complete. This ensures the signature
// matches what GCS expects while preserving the headers for the actual HTTP request.
package workload

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func IsGCS(endpoint string) bool {
	return strings.Contains(endpoint, ".googleapis.com")
}

type skipHeadersKey struct{}

func skipHeaders(headers []string) middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(
		"SkipHeaders",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
			req, ok := in.Request.(*smithyhttp.Request)
			if !ok {
				return out, metadata, &v4.SigningError{Err: fmt.Errorf("skipHeaders: unexpected middleware type %T", in.Request)}
			}
			s := make(map[string]string, len(headers))
			for _, h := range headers {
				// Case-insensitive header lookup
				val := req.Header.Get(h)
				if val != "" {
					s[h] = val
					req.Header.Del(h)
				}
			}
			ctx = middleware.WithStackValue(ctx, skipHeadersKey{}, s)
			return next.HandleFinalize(ctx, in)
		},
	)
}

func restoreSkipped() middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(
		"RestoreSkipped",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
			req, ok := in.Request.(*smithyhttp.Request)
			if !ok {
				return out, metadata, &v4.SigningError{Err: fmt.Errorf("restoreSkipped: unexpected middleware type %T", in.Request)}
			}
			s, _ := middleware.GetStackValue(ctx, skipHeadersKey{}).(map[string]string)
			for k, v := range s {
				req.Header.Set(k, v)
			}
			return next.HandleFinalize(ctx, in)
		},
	)
}

func FixSigningForGCS(o *s3.Options) {
	// Set checksum calculation to 'when_required' for GCS compatibility
	// AWS SDK v2 changed defaults which broke compatibility with GCS
	// See: https://www.beginswithdata.com/2025/05/14/aws-s3-tools-with-gcs/
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired

	o.UsePathStyle = true

	// Headers that must be excluded from signature calculation for GCS compatibility
	// GCS's S3 implementation doesn't expect these headers in the signature
	// See: https://github.com/aws/aws-sdk-go-v2/issues/1816
	headers := []string{
		"Accept-Encoding",
	}
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		// Insert before signing to remove headers from signature calculation
		if err := stack.Finalize.Insert(skipHeaders(headers), "Signing", middleware.Before); err != nil {
			return err
		}

		// Insert after signing to restore headers for the actual HTTP request
		return stack.Finalize.Insert(restoreSkipped(), "Signing", middleware.After)
	})
}
