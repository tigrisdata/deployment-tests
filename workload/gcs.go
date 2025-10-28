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
				s[h] = req.Header.Get(h)
				req.Header.Del(h)
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
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	headers := []string{"Accept-Encoding"}
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		if err := stack.Finalize.Insert(skipHeaders(headers), "Signing", middleware.Before); err != nil {
			return err
		}

		return stack.Finalize.Insert(restoreSkipped(), "Signing", middleware.After)
	})
}
