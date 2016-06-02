# -*- coding: utf-8 -*-
"""
Created on Wed Jun 25 09:39:45 2014

@author: joseph salmon
"""

###############################################################################
###############################################################################
###########################      Sequence 1      ##############################
###############################################################################
###############################################################################

import numpy as np
import matplotlib.pyplot as plt #for plots
from matplotlib import rc


###################################################################################
# Plot initialization

## Uncomment to save
#
#dirname="../srcimages/"
#imageformat='.svg'

rc('font', **{'family':'sans-serif', 'sans-serif':['Computer Modern Roman']})
params = {'axes.labelsize': 12,
          'text.fontsize': 12,
          'legend.fontsize': 12,
          'xtick.labelsize': 10,
          'ytick.labelsize': 10,
          'text.usetex': True,
          'figure.figsize': (8,6)}
plt.rcParams.update(params)
mc3my_brown = (0.64,0.16,0.16)
purple = (148./255,0,211./255)
plt.close("all")

###################################################################################
#moyenne empirique:
mu = 1.5
sigma = 4
nb_samples = 8

np.random.seed(seed=2)
rgamma = np.random.gamma
X = rgamma( mu, sigma , nb_samples )#mu,sigma)`
y=np.ones(nb_samples,)
# Statistiques:
meanX = np.mean(X)
minX = np.min(X)
maxX = np.max(X)
medX = np.median(X)
MADX = np.median(np.abs(X-medX))
s = np.std(X)

fig1, ax = plt.subplots(figsize=(10,3))
ax.set_ylim(0,1.5)
ax.set_xlim(minX- 0.1*np.ptp(X) ,maxX + 0.1*np.ptp(X))
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)

ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)

ax.plot([meanX,meanX],[0,1.5], color =mc3my_brown, linewidth=1.5, linestyle="--")
plt.xlabel(r'$y$',fontsize=18)
plt.annotate(r'$\bar{y}_n : \mbox{moyenne empirique}$',xy=(meanX, 0.4), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=18,color =mc3my_brown)

plt.tight_layout()
plt.show()

## Uncomment to save
#
#filename="GammaSampleMean"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)




###################################################################################
#mediane empirique:

fig1, ax = plt.subplots(figsize=(10,3))
ax.set_ylim(0,1.5)
ax.set_xlim(minX- 0.1*np.ptp(X) ,maxX + 0.1*np.ptp(X))
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)

ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
ax.plot([medX,medX],[0,1.5], color =purple, linewidth=1.5, linestyle="--")
plt.xlabel(r'$y$',fontsize=18)
plt.annotate(r'$\rm{Med}_n(\mathbb{y}): \mbox{m\'ediane empirique}$',xy=(medX, 1), xycoords='data', xytext=(-210, +30), textcoords='offset points', fontsize=18,color =purple)

plt.tight_layout()
plt.show()

## Uncomment to save
#
#filename="GammaSampleMediane"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)




###################################################################################
#moyenne / mediane empirique:
fig1, ax = plt.subplots(figsize=(10,3))
ax.set_ylim(0,1.5)
ax.set_xlim(minX- 0.1*np.ptp(X) ,maxX + 0.1*np.ptp(X))
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)

ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
ax.plot([meanX,meanX],[0,1.5], color =mc3my_brown, linewidth=1.5, linestyle="--")
ax.plot([medX,medX],[0,1.5], color =purple, linewidth=1.5, linestyle="--")

plt.xlabel(r'$y$',fontsize=18)
plt.annotate(r'$\rm{Med}_n(\mathbb{y}): \mbox{m\'ediane empirique}$',xy=(medX, 1), xycoords='data', xytext=(-210, +30), textcoords='offset points', fontsize=18,color =purple)
plt.annotate(r'$\bar{y}_n : \mbox{moyenne empirique}$',xy=(meanX, 0.4), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=18,color =mc3my_brown)

plt.tight_layout()
plt.show()

## Uncomment to save
#
#filename="GammaSampleMedianeMean"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)



###################################################################################
fig1, ax = plt.subplots(figsize=(10,3))
ax.set_ylim(0,1.5)
ax.set_xlim(minX- 0.1*np.ptp(X) ,maxX + 0.1*np.ptp(X))
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)

ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)

ax.plot([meanX,meanX],[0,1.5], color =mc3my_brown, linewidth=1.5, linestyle="--")
plt.arrow(meanX, 1.2, -s,0, fc=mc3my_brown, ec=mc3my_brown, head_width=0.05, head_length=0.1,length_includes_head=True )
plt.arrow(meanX-s, 1.2, s,0, fc=mc3my_brown, ec=mc3my_brown, head_width=0.05, head_length=0.1,length_includes_head=True )
plt.arrow(meanX, 1.2, s,0, fc=mc3my_brown, ec=mc3my_brown, head_width=0.05, head_length=0.1,length_includes_head=True )
plt.arrow(meanX+s, 1.2, -s,0, fc=mc3my_brown, ec=mc3my_brown, head_width=0.05, head_length=0.1,length_includes_head=True )



plt.xlabel(r'$y$',fontsize=18)
plt.annotate(r'$\bar{y}_n : \mbox{moyenne empirique}$',xy=(meanX, 0.4), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=18,color =mc3my_brown)
plt.annotate(r'$s_n$',xy=(meanX+s*(0.4), 1), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=18,color =mc3my_brown)
plt.annotate(r'$s_n$',xy=(meanX-s*(0.6), 1), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=18,color =mc3my_brown)

plt.tight_layout()
plt.show()

## Uncomment to save
#
#filename="GammaSD"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)





###################################################################################
fig1, ax = plt.subplots(figsize=(10,3))
ax.set_ylim(0,1.5)
ax.set_xlim(minX- 0.1*np.ptp(X) ,maxX + 0.1*np.ptp(X))
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)

ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)

ax.plot([medX,medX],[0,1.5], color =purple, linewidth=1.5, linestyle="--")

plt.arrow(medX, 1.2, -MADX,0, fc=purple, ec=purple, head_width=0.05, head_length=0.1,length_includes_head=True )
plt.arrow(medX-MADX, 1.2, MADX,0, fc=purple, ec=purple, head_width=0.05, head_length=0.1,length_includes_head=True )
plt.arrow(medX, 1.2, MADX,0, fc=purple, ec=purple, head_width=0.05, head_length=0.1,length_includes_head=True )
plt.arrow(medX+MADX, 1.2, -MADX,0, fc=purple, ec=purple, head_width=0.05, head_length=0.1,length_includes_head=True )



plt.xlabel(r'$y$',fontsize=18)
plt.annotate(r'$\rm{Med}_n(\mathbb{y}): \mbox{m\'ediane empirique}$',xy=(medX, 0.4), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=18,color =purple)
plt.annotate(r'$\rm{MAD}_n(\mathbb{y})$',xy=(medX+MADX*(0.1), 1), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=14,color =purple)
plt.annotate(r'$\rm{MAD}_n(\mathbb{y})$',xy=(medX-MADX*(1.2), 1), xycoords='data', xytext=(+10, +30), textcoords='offset points', fontsize=14,color =purple)

plt.tight_layout()
plt.show()

## Uncomment to save
#
#filename="GammaMAD"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)



###################################################################################
#histogramme:
mu = 1
sigma = 3
nb_samples = 30

np.random.seed(seed=1)
rgamma = np.random.gamma
X = rgamma( mu, sigma , nb_samples )#mu,sigma)`
y=np.ones(nb_samples,)
# Statistiques:
meanX = np.mean(X)
minX = np.min(X)
maxX = np.max(X)
medX = np.median(X)
MADX = np.median(np.abs(X-medX))
s = np.std(X)
sorted_data=np.sort(X)


fig1 = plt.figure(figsize=(20, 6))
plt.subplots_adjust(hspace=0.3)
ax = fig1.add_subplot(211)
ax.set_ylim(0,1.5)
range_lim=(-0.5,7.5)#0, X.max()+0.3
ax.set_xlim(range_lim)
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)
ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
plt.xlabel(r'$y$',fontsize=18)
plt.suptitle(r"$\mbox{Nombre d'\'echantillons}"+":n={0}$".format(nb_samples),multialignment='center')

ax2 = fig1.add_subplot(212)
ax2.set_xlim(range_lim)
plt.hist(X,bins=10,normed=True,align='mid',color=purple)
plt.ylabel(r'$\mbox{Fr\'equence}$',fontsize=18)
plt.xlabel(r'$y$',fontsize=18)

plt.show()

## Uncomment to save
#
#filename="GammaHist"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)


###################################################################################
# fonction de repartition:

fig1 = plt.figure(figsize=(20, 6))
plt.subplots_adjust(hspace=0.3)
ax = fig1.add_subplot(211)
ax.set_ylim(0,1.5)
ax.set_xlim(range_lim)
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)
ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
plt.xlabel(r'$y$',fontsize=18)
plt.suptitle(r"$\mbox{Nombre d'\'echantillons}"+":n={0}$".format(nb_samples),multialignment='center')

ax2 = fig1.add_subplot(212)
ax2.set_xlim(range_lim)
plt.step(sorted_data, np.arange(sorted_data.size,dtype='float')/nb_samples,color=purple)
#plt.hist(X,bins=200,cumulative=True, normed=True,range=range_lim,histtype='step',align='right')
plt.ylabel(r'$\mbox{Fr\'equence cumul\'e}$',fontsize=18)
plt.xlabel(r'$y$',fontsize=18)

plt.show()

## Uncomment to save
#
#filename="Gammaecdf"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)

###################################################################################
# fonction quantile:

fig1 = plt.figure(figsize=(20, 6))
plt.subplots_adjust(hspace=0.3)
ax = fig1.add_subplot(211)
ax.set_ylim(0,1.5)
ax.set_xlim(range_lim)
ax.get_xaxis().tick_bottom()
ax.axes.get_yaxis().set_visible(False)
ax.spines['right'].set_color('none')
ax.spines['top'].set_color('none')
ax.spines['bottom'].set_position(('data',0.5))
ax.spines['left'].set_color('none')

ax.scatter(X, y,color='black',s=300, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
plt.xlabel(r'$y$',fontsize=18)
plt.suptitle(r"$\mbox{Nombre d'\'echantillons}"+":n={0}$".format(nb_samples),multialignment='center')

ax2 = fig1.add_subplot(212)
ax2.set_xlim(range_lim)

p=0.44
q=np.percentile(X,p*100)

ax2.plot([q,range_lim[0]],[p,p], color =mc3my_brown, linewidth=1.5, linestyle="--")
ax2.plot([q,q],[0,p], color =mc3my_brown, linewidth=1.5, linestyle="--")
ax2.annotate(r'$p=%.2f$'%p,xy=(0, p), xycoords='data', xytext=(-23, +6), textcoords='offset points', fontsize=18,color = mc3my_brown)
ax2.annotate(r'$F_n^\leftarrow(p)=%.2f$'%q,xy=(q, 0), xycoords='data', xytext= (-15, -30) , textcoords='offset points', fontsize=18,color = mc3my_brown)



p=0.87
q=np.percentile(X,p*100)

ax2.plot([q,range_lim[0]],[p,p], color =mc3my_brown, linewidth=1.5, linestyle="--")
ax2.plot([q,q],[0,p], color =mc3my_brown, linewidth=1.5, linestyle="--")
ax2.annotate(r'$p=%.2f$'%p,xy=(0, p), xycoords='data', xytext=(-23, +6), textcoords='offset points', fontsize=18,color = mc3my_brown)
ax2.annotate(r'$F_n^\leftarrow(p)=%.2f$'%q,xy=(q, 0), xycoords='data', xytext= (-15, -30) , textcoords='offset points', fontsize=18,color = mc3my_brown)




plt.step(sorted_data, np.arange(sorted_data.size,dtype='float')/nb_samples,color=purple)
plt.ylabel(r'$\mbox{Fr\'equence cumul\'e}$',fontsize=18)
plt.xlabel(r'$y$',fontsize=18)

plt.show()

## Uncomment to save
#
#filename="GammaQuantiles"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)



###################################################################################
# Correlations desssin
rng = np.random.RandomState(42)     # initializing  the randomness
n_samples=90
sig_list = [-1,-0.8,-.4,0,0.4,0.8,1]
nb_sig = len(sig_list)

#plt.subplots_adjust(hspace=2)
fig1 = plt.figure(figsize=(nb_sig, 1.5))
for i in xrange(nb_sig):
    MySigma = np.eye(2,2)+np.array([[0,sig_list[i-1]],[sig_list[i-1],0]])
    X = rng.multivariate_normal(np.array([0,0]), MySigma, n_samples)
    ax = fig1.add_subplot(1,nb_sig,i)
    plt.title(r" ${0}$".format(sig_list[i-1]))
    ax.axes.get_yaxis().set_visible(False)
    ax.axes.get_xaxis().set_visible(False)
    ax.scatter(X[:,0], X[:,1],color='black',s=2, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)

plt.tight_layout()

## Uncomment to save
#
#filename="Correlations2Dessins"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)


###################################################################################
# Correlations dessins negatif
rng = np.random.RandomState(42)     # initializing  the randomness
n_samples=90
theta_list=[np.pi*1/16,np.pi*2/16,np.pi*3/16,np.pi*4/16,np.pi*5/16,np.pi*6/16,np.pi*7/16]
nb_theta = len(theta_list)

fig1 = plt.figure(figsize=(nb_theta, 1.5))
#plt.subplots_adjust(hspace=0.1)
D = np.diag([1,0])
for i in xrange(nb_theta):
    theta=theta_list[i]
    P = np.array([[np.cos(theta), np.sin(theta)],[-np.sin(theta), np.cos(theta)]])
    MySigma= np.dot(np.dot(P,D),np.transpose(P))
    X = rng.multivariate_normal(np.array([0,0]), MySigma, n_samples)
    ax = fig1.add_subplot(1,nb_theta,i)
    ax.axes.get_yaxis().set_visible(False)
    ax.axes.get_xaxis().set_visible(False)
    ax.scatter(X[:,0], X[:,1],color='black',s=2, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
    ax.set_ylim(-3,3)
    ax.set_xlim(-3,3)
    corr_mat=np.corrcoef(X[:,0],X[:,1])
    corr_mat[0,1]
    #plt.title(r"$-1$")
    plt.title(r"${0}$".format(corr_mat[0,1]))

plt.tight_layout()

## Uncomment to save
#
#filename="Correlations2Dessins_bis"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)

###################################################################################
# Correlations dessins  positif
rng = np.random.RandomState(42)     # initializing  the randomness
n_samples=90
theta_list=[np.pi*1/16,np.pi*2/16,np.pi*3/16,np.pi*4/16,np.pi*5/16,np.pi*6/16,np.pi*7/16]
nb_theta = len(theta_list)

fig1 = plt.figure(figsize=(nb_theta, 1.5))
#plt.subplots_adjust(hspace=0.1)
D = np.diag([1,0])
for i in xrange(nb_theta):
    theta=-theta_list[i]
    P = np.array([[np.cos(theta), np.sin(theta)],[-np.sin(theta), np.cos(theta)]])
    MySigma= np.dot(np.dot(P,D),np.transpose(P))
    X = rng.multivariate_normal(np.array([0,0]), MySigma, n_samples)
    ax = fig1.add_subplot(1,nb_theta,i)
    ax.axes.get_yaxis().set_visible(False)
    ax.axes.get_xaxis().set_visible(False)
    ax.scatter(X[:,0], X[:,1],color='black',s=2, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
    ax.set_ylim(-3,3)
    ax.set_xlim(-3,3)
    corr_mat=np.corrcoef(X[:,0],X[:,1])
    corr_mat[0,1]
    #plt.title(r"$-1$")
    plt.title(r"${0}$".format(corr_mat[0,1]))

plt.tight_layout()


## Uncomment to save
#
#filename="Correlations2Dessins_bis_pos"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)


###################################################################################
# Example zero correlation : melange de gaussiennes

rng = np.random.RandomState(42)     # initializing  the randomness
n_samples=100

fig1, ax = plt.subplots(figsize=(3,3))
MySigma = 0.01*np.eye(2,2)
X1 = rng.multivariate_normal(np.array([0,1]), MySigma, n_samples)
X2 = rng.multivariate_normal(np.array([1,0]), MySigma, n_samples)
X3 = rng.multivariate_normal(np.array([1,1]), MySigma, n_samples)
X4 = rng.multivariate_normal(np.array([0,0]), MySigma, n_samples)
Z=np.vstack((X1, X2))
Y=np.vstack((X3 ,X4))
X=np.vstack((Z ,Y))
ax.axes.get_yaxis().set_visible(False)
ax.axes.get_xaxis().set_visible(False)
ax.scatter(X[:,0],X[:,1],color='black',s=2, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
ax.set_ylim(-1,2)
ax.set_xlim(-1,2)
corr_mat=np.corrcoef(X[:,0],X[:,1])
debut_titre=r"$\mbox{Corr\'elation }$"
plt.title(debut_titre+r"$ = %.3f$"%corr_mat[0,1])

## Uncomment to save
#
#filename="Correlations_4MixtGauss"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)

###################################################################################
# Example zero correlation: cercle
rng = np.random.RandomState(42)     # initializing  the randomness
n_samples=400

fig1, ax = plt.subplots(figsize=(3,3))
MySigma = 0.01*np.eye(2,2)
r = 0.8+0.4*rng.rand(1,n_samples)
thetas= 2*np.pi/n_samples*np.arange(n_samples)
P = np.transpose(np.array(np.vstack((np.cos(thetas), np.sin(thetas)))))


ax.axes.get_yaxis().set_visible(False)
ax.axes.get_xaxis().set_visible(False)
ax.scatter(np.multiply(r,P[:,0]),r*P[:,1],color='black',s=2, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
ax.set_ylim(-2,2)
ax.set_xlim(-2,2)
corr_mat=np.corrcoef(r*P[:,0],r*P[:,1])
debut_titre=r"$\mbox{Corr\'elation }$"
plt.title(debut_titre+r"$ = %.3f$"%corr_mat[0,1])


## Uncomment to save
#
#filename="Correlations_Cercle"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)


###################################################################################
# Example zero correlation: carr\'e
rng = np.random.RandomState(42)     # initializing  the randomness
n_samples=400

fig1, ax = plt.subplots(figsize=(3,3))
X1=rng.rand(1,n_samples)
X2=rng.rand(1,n_samples)

ax.axes.get_yaxis().set_visible(False)
ax.axes.get_xaxis().set_visible(False)
ax.scatter(X1,X2,color='black',s=2, c=purple,edgecolors=mc3my_brown, marker='o',faceted=True)
ax.set_ylim(-1,2)
ax.set_xlim(-1,2)
corr_mat=np.corrcoef(X1,X2)
debut_titre=r"$\mbox{Corr\'elation }$"
plt.title(debut_titre+r"$ = %.3f$"%corr_mat[0,1])

## Uncomment to save
#
#filename="Correlations_Carre"
#image_name=dirname+filename+imageformat
#fig1.savefig(image_name)




##############################################################################
######      scatter matrix
##############################################################################
#restart here to avoid an issue with underscore in variables names

import seaborn as sns
## Seaborn help
#http://web.stanford.edu/~mwaskom/software/seaborn/tutorial/axis_grids.html


iris = sns.load_dataset("iris")
g = sns.PairGrid(iris, hue="species", palette="colorblind")
g.map_diag(plt.hist)
g.map_offdiag(plt.scatter)
g.add_legend()


## Uncomment to save
#
#filename="scatter_matrix"
#image_name=dirname+filename+imageformat
#plt.savefig(image_name)
