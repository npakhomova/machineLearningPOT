"""Simple image classification with Inception.
Run image classification with Inception trained on ImageNet 2012 Challenge data
set.
This program creates a graph from a saved GraphDef protocol buffer,
and runs inference on an input JPEG image. It outputs human readable
strings of the top 5 predictions along with their probabilities.
Change the --image_file argument to any jpg image to compute a
classification of that image.
Please see the tutorial and website for a detailed description of how
to use this script to perform image recognition.
https://tensorflow.org/tutorials/image_recognition/
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os.path
import re
import sys
import tarfile

import numpy as np
from six.moves import urllib
import tensorflow as tf
import json
from pprint import pprint
import os.path
import collections


FLAGS = tf.app.flags.FLAGS

# classify_image_graph_def.pb:
#   Binary representation of the GraphDef protocol buffer.
# imagenet_synset_to_human_label_map.txt:
#   Map from synset ID to a human readable string.
# imagenet_2012_challenge_label_map_proto.pbtxt:
#   Text representation of a protocol buffer mapping a label to synset ID.
tf.app.flags.DEFINE_string(
    'model_dir', '/Users/npakhomova/codebase/css/machineLearningPOT/dressLengthModel/',
    """Path to classify_image_graph_def.pb, """
    """imagenet_synset_to_human_label_map.txt, and """
    """imagenet_2012_challenge_label_map_proto.pbtxt.""")

tf.app.flags.DEFINE_integer('num_top_predictions', 2,
                            """Display this many predictions.""")




class NodeLookup(object):
  """Converts integer node ID's to human readable labels."""

  def __init__(self,
               label_lookup_path=None,
               uid_lookup_path=None):

    if not uid_lookup_path:
      uid_lookup_path = os.path.join(
          FLAGS.model_dir, 'output_labels.txt')
    self.node_lookup = self.read_labels(uid_lookup_path) #self.load(label_lookup_path, uid_lookup_path)


  def read_labels(self, label_lookup_path):

    # Loads mapping from string UID to human-readable string
    proto_as_ascii_lines = tf.gfile.GFile(label_lookup_path).readlines()
    id_to_human = {}
    p = re.compile(r'[n\d]*[ \S,]*')
    for i,line in enumerate(proto_as_ascii_lines):
      id_to_human[i] = line[:-1]

    return id_to_human

  def load(self, label_lookup_path, uid_lookup_path):
    """Loads a human readable English name for each softmax node.
    Args:
      label_lookup_path: string UID to integer node ID.
      uid_lookup_path: string UID to human-readable string.
    Returns:
      dict from integer node ID to human-readable string.
    """
    if not tf.gfile.Exists(uid_lookup_path):
      tf.logging.fatal('File does not exist %s', uid_lookup_path)
    if not tf.gfile.Exists(label_lookup_path):
      tf.logging.fatal('File does not exist %s', label_lookup_path)

    # Loads mapping from string UID to human-readable string
    proto_as_ascii_lines = tf.gfile.GFile(uid_lookup_path).readlines()
    uid_to_human = {}
    p = re.compile(r'[n\d]*[ \S,]*')
    for line in proto_as_ascii_lines:
      parsed_items = p.findall(line)
      uid = parsed_items[0]
      human_string = parsed_items[2]
      uid_to_human[uid] = human_string

    # Loads mapping from string UID to integer node ID.
    node_id_to_uid = {}
    proto_as_ascii = tf.gfile.GFile(label_lookup_path).readlines()
    for line in proto_as_ascii:
      if line.startswith('  target_class:'):
        target_class = int(line.split(': ')[1])
      if line.startswith('  target_class_string:'):
        target_class_string = line.split(': ')[1]
        node_id_to_uid[target_class] = target_class_string[1:-2]

    # Loads the final mapping of integer node ID to human-readable string
    node_id_to_name = {}
    for key, val in node_id_to_uid.items():
      if val not in uid_to_human:
        tf.logging.fatal('Failed to locate: %s', val)
      name = uid_to_human[val]
      node_id_to_name[key] = name

    return node_id_to_name

  def id_to_string(self, node_id):
    if node_id not in self.node_lookup:
      return ''
    return self.node_lookup[node_id]


def create_graph():
  """Creates a graph from saved GraphDef file and returns a saver."""
  # Creates graph from saved graph_def.pb.

  # with tf.gfile.FastGFile(os.path.join(
  #     FLAGS.model_dir, 'classify_image_graph_def.pb'), 'rb') as f:
  with tf.gfile.FastGFile(os.path.join(
      FLAGS.model_dir, 'output_graph.pb'), 'rb') as f:
    graph_def = tf.GraphDef()
    graph_def.ParseFromString(f.read())
    _ = tf.import_graph_def(graph_def, name='')


def run_inference_on_image(image):
  """Runs inference on an image.
  Args:
    image: Image file name.
  Returns:
    Nothing
  """
  if not tf.gfile.Exists(image):
    tf.logging.fatal('File does not exist %s', image)
  image_data = tf.gfile.FastGFile(image, 'rb').read()



  with tf.Session() as sess:
    # Some useful tensors:
    # 'softmax:0': A tensor containing the normalized prediction across
    #   1000 labels.
    # 'pool_3:0': A tensor containing the next-to-last layer containing 2048
    #   float description of the image.
    # 'DecodeJpeg/contents:0': A tensor containing a string providing JPEG
    #   encoding of the image.
    # Runs the softmax tensor by feeding the image_data as input to the graph.

    # Creates node ID --> English string lookup.
    node_lookup = NodeLookup()

    softmax_tensor = sess.graph.get_tensor_by_name('final_result:0')
    predictions = sess.run(softmax_tensor,
                           {'DecodeJpeg/contents:0': image_data})
    predictions = np.squeeze(predictions)



    top_k = predictions.argsort()[-FLAGS.num_top_predictions:][::-1]
    result =[]
    for node_id in top_k:
      human_string = node_lookup.id_to_string(node_id)
      score = predictions[node_id]

      point = collections.namedtuple('Point', ['humanString', 'score'])
      point.humanString = human_string
      point.score = score
      result.append(point)
    return result


create_graph()
dressLength = ['AbovetheKnee', 'Mini', 'Long_Maxi','Midi_Tea_Length','KneeLength']
for length in dressLength:
    with open('/Users/npakhomova/codebase/css/machineLearningPOT/dressLengthOnlyPublishedBCOM/bcomDresses'+length+'.json','r') as data_file:
      data = json.load(data_file)
      for row in data:
          if 'imagePath' in row:
            jpg_ = row['imagePath']
            if (os.path.isfile(jpg_)):
                imageResult = run_inference_on_image(jpg_)
                row['cvResult'] =  str(imageResult[0].humanString)
                row['cvScore'] = format(imageResult[0].score, '.2f')
                row['cvResult2'] = str(imageResult[1].humanString)
                row['cvScore2'] = format(imageResult[1].score, '.2f')
                if length == 'AbovetheKnee':
                    row['warning'] = not (('short' == row['cvResult']) | ('knee length' == row['cvResult']) | ('high low' == row['cvResult'])) #short validation  high low
                if length == 'Mini':
                    row['warning'] = not ('short' == row['cvResult'])  # long validation
                if length == 'Long_Maxi':
                    row['warning'] = not ('long' == row['cvResult']) # mid validation
                if length == 'Midi_Tea_Length':
                    row['warning'] = not (('below the knee' == row['cvResult']) | ('high low' == row['cvResult']))  # mid validation high low
                if length == 'KneeLength':
                    row['warning'] = not (('below the knee' == row['cvResult']) | ('knee length' == row['cvResult']) | ('high low' == row['cvResult']))  # mid validation
    with open('/Users/npakhomova/codebase/css/machineLearningPOT/dressLengthOnlyPublishedBCOM/bcomDresses'+length+'Processed.json','w') as outfile:
        json.dump(data, outfile)









