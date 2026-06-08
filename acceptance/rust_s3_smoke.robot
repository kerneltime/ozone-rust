*** Settings ***
Documentation       S3 acceptance tests for the Rust Apache Ozone S3 gateway, run via the
...                 aws CLI against the live Rust stack (gateway + 5 EC datanodes +
...                 compliant OM fixture). Operation coverage mirrors Apache Ozone's own
...                 s3 smoketests (objectputget / bucketcreate / objectdelete /
...                 MultipartUpload / objectcopy). Auth is trust-the-proxy: the gateway
...                 takes the access key as the principal and does NOT verify SigV4, so the
...                 signature-verification cases from the upstream suite are intentionally
...                 omitted.
Library             OperatingSystem
Library             String
Suite Setup         Configure AWS

*** Variables ***
${ENDPOINT}         http://127.0.0.1:9878
${BUCKET}           robotbucket

*** Keywords ***
Configure AWS
    Set Environment Variable    AWS_ACCESS_KEY_ID        robotuser
    Set Environment Variable    AWS_SECRET_ACCESS_KEY    secret
    Set Environment Variable    AWS_DEFAULT_REGION       us-east-1

S3Api
    [Arguments]    ${cmd}
    ${rc}    ${out} =    Run And Return Rc And Output    aws --endpoint-url ${ENDPOINT} s3api ${cmd}
    Should Be Equal As Integers    ${rc}    0    aws s3api failed (rc=${rc}): ${out}
    RETURN    ${out}

S3Api Should Fail
    [Arguments]    ${cmd}    ${expect}
    ${rc}    ${out} =    Run And Return Rc And Output    aws --endpoint-url ${ENDPOINT} s3api ${cmd}
    Should Not Be Equal As Integers    ${rc}    0    expected failure, succeeded: ${out}
    Should Contain    ${out}    ${expect}

*** Test Cases ***
Create Bucket
    S3Api    create-bucket --bucket ${BUCKET}

Head Existing Bucket
    S3Api    head-bucket --bucket ${BUCKET}

Head Missing Bucket Returns 404
    S3Api Should Fail    head-bucket --bucket nosuchbucket-xyz    Not Found

List Buckets Includes The Bucket
    ${out} =    S3Api    list-buckets
    Should Contain    ${out}    ${BUCKET}

Put And Get Object Round Trips
    Create File    /tmp/r_obj.txt    hello-ozone-rust-gateway
    S3Api    put-object --bucket ${BUCKET} --key dir/obj.txt --body /tmp/r_obj.txt
    Remove File    /tmp/r_obj.out
    S3Api    get-object --bucket ${BUCKET} --key dir/obj.txt /tmp/r_obj.out
    ${got} =    Get File    /tmp/r_obj.out
    Should Be Equal    ${got}    hello-ozone-rust-gateway

Head Object Reports Size
    ${out} =    S3Api    head-object --bucket ${BUCKET} --key dir/obj.txt
    Should Contain    ${out}    "ContentLength": 24

Put Zero Byte Object
    Create File    /tmp/r_zero.txt    ${EMPTY}
    S3Api    put-object --bucket ${BUCKET} --key dir/zero --body /tmp/r_zero.txt
    ${out} =    S3Api    head-object --bucket ${BUCKET} --key dir/zero
    Should Contain    ${out}    "ContentLength": 0

List Objects With Prefix
    ${out} =    S3Api    list-objects-v2 --bucket ${BUCKET} --prefix dir/
    Should Contain    ${out}    dir/obj.txt
    Should Contain    ${out}    dir/zero

Get Missing Object Returns NoSuchKey
    S3Api Should Fail    get-object --bucket ${BUCKET} --key dir/missing /tmp/none    NoSuchKey

Multipart Upload Round Trips
    ${p1} =    Evaluate    "A" * 6291456
    ${p2} =    Evaluate    "B" * 1048576
    Create File    /tmp/r_p1    ${p1}
    Create File    /tmp/r_p2    ${p2}
    ${up} =    S3Api    create-multipart-upload --bucket ${BUCKET} --key mp/big --query UploadId --output text
    ${up} =    Strip String    ${up}
    ${e1} =    S3Api    upload-part --bucket ${BUCKET} --key mp/big --part-number 1 --upload-id ${up} --body /tmp/r_p1 --query ETag --output text
    ${e2} =    S3Api    upload-part --bucket ${BUCKET} --key mp/big --part-number 2 --upload-id ${up} --body /tmp/r_p2 --query ETag --output text
    ${e1} =    Replace String    ${e1}    "    ${EMPTY}
    ${e2} =    Replace String    ${e2}    "    ${EMPTY}
    ${e1} =    Strip String    ${e1}
    ${e2} =    Strip String    ${e2}
    ${etag} =    S3Api    complete-multipart-upload --bucket ${BUCKET} --key mp/big --upload-id ${up} --multipart-upload "Parts=[{ETag=${e1},PartNumber=1},{ETag=${e2},PartNumber=2}]" --query ETag --output text
    Should Contain    ${etag}    -2
    Remove File    /tmp/r_big.out
    S3Api    get-object --bucket ${BUCKET} --key mp/big /tmp/r_big.out
    ${size} =    Get File Size    /tmp/r_big.out
    Should Be Equal As Integers    ${size}    7340032

List Parts Of An Upload
    ${up} =    S3Api    create-multipart-upload --bucket ${BUCKET} --key mp/two --query UploadId --output text
    ${up} =    Strip String    ${up}
    S3Api    upload-part --bucket ${BUCKET} --key mp/two --part-number 1 --upload-id ${up} --body /tmp/r_p1
    ${out} =    S3Api    list-parts --bucket ${BUCKET} --key mp/two --upload-id ${up}
    Should Contain    ${out}    "PartNumber": 1
    S3Api    abort-multipart-upload --bucket ${BUCKET} --key mp/two --upload-id ${up}

Copy Object
    S3Api    copy-object --bucket ${BUCKET} --key dir/copy.txt --copy-source ${BUCKET}/dir/obj.txt
    Remove File    /tmp/r_copy.out
    S3Api    get-object --bucket ${BUCKET} --key dir/copy.txt /tmp/r_copy.out
    ${got} =    Get File    /tmp/r_copy.out
    Should Be Equal    ${got}    hello-ozone-rust-gateway

Delete Object
    S3Api    delete-object --bucket ${BUCKET} --key dir/obj.txt
    S3Api Should Fail    head-object --bucket ${BUCKET} --key dir/obj.txt    Not Found

Batch Delete Objects
    S3Api    delete-objects --bucket ${BUCKET} --delete Objects=[{Key=dir/zero},{Key=dir/copy.txt},{Key=mp/big}]
    ${out} =    S3Api    list-objects-v2 --bucket ${BUCKET} --prefix dir/
    Should Not Contain    ${out}    dir/zero
