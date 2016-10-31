# coding: utf-8

"""
	author: Loïc M. DIVAD
	date: 2016-10-25
	see also: https://www.kaggle.com/c/digit-recognizer
"""

# ### imports
import logging
import numpy as np
import pandas as pd
import tensorflow as tf

from optparse import OptionParser
from sklearn.cross_validation import train_test_split

# ### config
optparser = OptionParser()
optparser.add_option("-l", "--logginglvl", dest="logginglvl", default="info", help="Logging level")
optparser.add_option("-t", "--trainmodel", dest="trainmodel", default=1, type="int", help="Fit the cnn or not")
optparser.add_option("-u", "--upgratings", dest="upgratings", default=0, type="int", help="Compute progress or not")
optparser.add_option("-v", "--validation", dest="validation", default=0.20, type="float", help="Size of the validation set")
optparser.add_option("-s", "--batchsizes", dest="batchsizes", default=50, type="int", help="Size of the batches passes throught the optimisation")

options, args = optparser.parse_args()

# ### constant
step = options.batchsizes
inf, sup = (0, step)
TRAINING_LAPS = 60001


LOG_FORMAT = '%(asctime)s [ %(levelname)s ] : %(message)s'
LVL_MAP = {
 'debug'	:	logging.DEBUG, 
 'info'		:	logging.INFO, 
 'warn'		:	logging.WARN, 
 'error'	:	logging.ERROR, 
 'fatal'	:	logging.FATAL
}

# ### logging
logging.basicConfig(format=LOG_FORMAT)
log = logging.getLogger()
log.setLevel(LVL_MAP[options.logginglvl])

# ### functions
def output(label, num_output=10):
    y = np.zeros(num_output)
    np.put(y, label, 1)
    return y.tolist()
    
def outputLayer(labels, num_output=10):
    return np.array(map(lambda y: output(y), labels))

def nextBatch(inf, sup, step, maxSize):
    if maxSize <= sup + step:
        return (0, step)
    
    return (inf + step, sup + step)

def to_digit(df, idx, i=28, j=28):
    return df.iloc[idx,:].reshape((i, j))

# tensor factories
# Weight Initialization
def weight_variable(shape):
    """
    returns a placeholders of the specified shape
    filled with initials random truncated normal values. 
    """
    
    initial = tf.truncated_normal(shape, stddev=0.1)
    return tf.Variable(initial)

def bias_variable(shape):
    """ returns a placeholders of  bais initilazed at 0.1"""
    
    initial = tf.constant(0.1, shape=shape)
    return tf.Variable(initial)

# Convolution and Pooling
def conv2d(x, W):
    return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding='SAME')

def max_pool_2x2(x):
    return tf.nn.max_pool(x, ksize=[1, 2, 2, 1],
                        strides=[1, 2, 2, 1], padding='SAME')

# ### Datasets
DATA = ( pd.DataFrame.from_csv("data/train.csv")
	.apply(lambda d: d / 255.0)
	.astype(np.float32)
)

log.debug("Splitting the dataset at %s %%"%(options.validation * 100))
DATA_TRAIN, DATA_VALID = train_test_split(DATA, test_size=options.validation, random_state=42)

DATA_TEST = ( pd.DataFrame.from_csv("data/test.csv", index_col=None)
	.apply(lambda d: d / 255.0)
	.astype(np.float32)
)

log.info("Size of the dataset DATA_TRAIN : %s"%len(DATA_TRAIN))
log.info("Size of the dataset DATA_VALID : %s"%len(DATA_VALID))

log.info("Size of the dataset DATA_TESTS : %s"%len(DATA_TEST))

log.debug("List of Nan values in the training set: %s"%filter(lambda y: y != 0, DATA.isnull().sum()))
log.debug("List of Nan values in the testing  set: %s"%filter(lambda y: y != 0, DATA_TEST.isnull().sum()))


# ### action

if __name__ == "__main__":

	log.info("Starting the algorithms and the tf session.")
	sess = tf.InteractiveSession() #tf.Session()

	x = tf.placeholder(tf.float32, shape=[None, 784])
	y_ = tf.placeholder(tf.float32, shape=[None, 10])

	#W = tf.Variable(tf.zeros([784,10]))
	#b = tf.Variable(tf.zeros([10]))

	W_conv1 = weight_variable([5, 5, 1, 32])
	b_conv1 = bias_variable([32])

	x_image = tf.reshape(x, [-1,28,28,1])

	h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + b_conv1)
	h_pool1 = max_pool_2x2(h_conv1)
	
	W_conv2 = weight_variable([5, 5, 32, 64])
	b_conv2 = bias_variable([64])

	h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)
	h_pool2 = max_pool_2x2(h_conv2)

	W_fc1 = weight_variable([7 * 7 * 64, 1024])
	b_fc1 = bias_variable([1024])

	h_pool2_flat = tf.reshape(h_pool2, [-1, 7*7*64])
	h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)

	keep_prob = tf.placeholder(tf.float32)
	h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)

	W_fc2 = weight_variable([1024, 10])
	b_fc2 = bias_variable([10])

	y_conv = tf.matmul(h_fc1_drop, W_fc2) + b_fc2

	cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(y_conv, y_))
	train_step = tf.train.AdamOptimizer(1e-4).minimize(cross_entropy)
	correct_prediction = tf.equal(tf.argmax(y_conv,1), tf.argmax(y_,1))
	accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

	log.info("Initilisation of all variables.")
	sess.run(tf.initialize_all_variables())

	if options.trainmodel == 1 :

		log.warn("Start fitting the model and training the cnn.")

		dataLenght = len(DATA_TRAIN)
		log.debug("- [INIT] - training : %i"%TRAINING_LAPS)
		log.debug("- [INIT] - training iterrations: %i"%TRAINING_LAPS)


		for i in range(TRAINING_LAPS):
			
			train_step.run(feed_dict = {
				x: DATA_TRAIN[inf:sup].as_matrix(),
				y_: outputLayer(DATA_TRAIN.index.tolist()[inf:sup]),
				keep_prob: 0.5
			})

			if i%2000 == 0:

				if options.upgratings == 1:

					train_acc = accuracy.eval(feed_dict={x: DATA_TRAIN.as_matrix(), y_: outputLayer(DATA_TRAIN.index.tolist()), keep_prob: 1.0})
					valid_acc = accuracy.eval(feed_dict={x: DATA_VALID.as_matrix(), y_: outputLayer(DATA_VALID.index.tolist()), keep_prob: 1.0})

					log.info("- [TRAINING UPDATES] - lap %i - training accuracy : \t %f"%(i,  train_acc))
					log.info("- [TRAINING UPDATES] - lap %i - validation accuracy : \t %f"%(i, valid_acc))
				else :
					log.info("- [TRAINING UPDATES] - lap %i -"%i)


			inf, sup = nextBatch(inf, sup, step, dataLenght)
			log.debug("Batche rotation [ %s : %s ]"%(inf, sup))


		prediction = tf.argmax(y_conv,1)

		# writing the result
		log.info("Writting down the prediction on the csv.")
		df_result = pd.DataFrame({'ImageId': range(1, len(PRED_LABEL)+1), 'Label': PRED_LABEL})
		df_result.to_csv('data/prediction_four.csv', index=False)

	else:

		log.fatal("Stop the script wihout training.")
