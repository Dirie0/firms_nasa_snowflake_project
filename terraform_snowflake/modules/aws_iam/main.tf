data "aws_iam_policy_document" "snowflake_trust_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_iam_user]
    }

  }
}

resource "aws_iam_role" "snowflake_s3_access_role" {
  name               = var.role_name
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust_policy.json
}

data "aws_iam_policy_document" "s3_read_policy" {
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["arn:aws:s3:::${var.bucket_name}/tables/*", "arn:aws:s3:::${var.bucket_name}/reference/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.bucket_name}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["tables/*", "reference/*"]
    }
  }
}

resource "aws_iam_role_policy" "s3_read_access" {
  name   = "snowflake-read-tables-reference"
  role   = aws_iam_role.snowflake_s3_access_role.id
  policy = data.aws_iam_policy_document.s3_read_policy.json
}
