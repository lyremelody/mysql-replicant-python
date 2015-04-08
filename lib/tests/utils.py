# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

def load_deployment(deployment):
    parts = deployment.split('.')
    pkg = __import__('.'.join(parts[:-1]), globals(), locals(), parts[-1:])
    return getattr(pkg, parts[-1])
