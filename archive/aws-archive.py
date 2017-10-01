#!/usr/bin/python

import os
import boto3
import json
import pwd
import grp
import argparse


# Parse command-line options
###########################################
def options():
    """Parse command-line options
    """
    parser = argparse.ArgumentParser(description="AWS data archiving tool. Uploads files to AWS and stores S3 metadata",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--files", help="Files to be archived.", required=True)
    parser.add_argument("-b", "--bucket", help="AWS S3 bucket name.", required=True)
    args = parser.parse_args()

    return args


# Main
###########################################
def main():
    """Main program.
    """
    args = options()

    # Create a client object for AWS S3
    s3 = boto3.resource('s3')
    # Connect to the specified S3 bucket
    bucket = s3.Bucket(args.bucket)

    if os.path.isfile(args.files):
        # If args.files points to a single file then archive it
        # Get the absolute path to the file to use this as the S3 key
        file = os.path.abspath(args.files)
        print("Archiving file {0}".format(file))
        archive_file(bucket=bucket, file=file)
    elif os.path.isdir(args.files):
        # If args.files points to a directory, then recursively walk through the provided path
        for root, dirs, files in os.walk(args.files):
            # We only need to worry about uploading files
            for filename in files:
                # Skip files that have the archived extension
                if filename[-8:] != "archived":
                    # Get the absolute path to the file to use this as the S3 key
                    file = os.path.join(os.path.abspath(root), filename)
                    print("Archiving file {0}".format(file))
                    archive_file(bucket=bucket, file=file)


def archive_file(bucket, file):
    # Get file metadata
    metadata = os.stat(file)
    # Open file
    data = open(file, "rb")
    # Upload the file to S3 and get a response object
    response = bucket.put_object(Body=data, Key=file, Metadata={"LastModified": str(metadata.st_mtime),
                                                                "Owner": pwd.getpwuid(metadata.st_uid).pw_name,
                                                                "Group": grp.getgrgid(metadata.st_gid).gr_name})
    data.close()
    # Save the response information to keep a record of the archived data
    properties = {"Bucket": response.bucket_name, "Key": response.key, "ETag": response.e_tag.replace('"', ""),
                  "ContentLength": response.content_length, "Metadata": response.metadata}
    arxiv = open(file + ".archived", "w")
    arxiv.write(json.dumps(properties, indent=4) + "\n")
    arxiv.close()


if __name__ == "__main__":
    main()
