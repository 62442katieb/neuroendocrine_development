# imports
import numpy as np
import pandas as pd
# this is one of mine, lives at https://github.com/62442katieb/ABCDWrangler
import abcdWrangler as abcdw

from os.path import join
from itertools import chain
from scipy.stats import spearmanr

ABCD_DIR = "/Volumes/projects_herting/LABDOCS/PROJECTS/ABCD/Data/release5.1/abcd-data-release-5.1"
PROJ_DIR = "/Volumes/projects_herting/LABDOCS/Personnel/Katie/kangaroo_aim1"
DATA_DIR = "data"
OUTP_DIR = "output"
FIGS_DIR = "figures"

# variables representing cortical morphology variables 
# that are significantly different between HC users and non-users
# denoted by the covariates included in brain-HC analyses
significant_brain = {    
    "none": [
        "smri_thick_cdk_paracnrh",
        "smri_thick_cdk_paracnlh",
        "smri_thick_cdk_suplrh",
        "smri_thick_cdk_precnlh",
        "smri_thick_cdk_ptcatelh",
        "smri_thick_cdk_mobfrlh",
        "smri_thick_cdk_sufrlh",
        "smri_thick_cdk_pcrh",
        "smri_thick_cdk_mobfrrh",
        "smri_thick_cdk_supllh",
        "smri_vol_cdk_precnlh",
        "smri_vol_cdk_paracnlh",
        "smri_vol_cdk_postcnlh",
        "smri_vol_cdk_precnrh"
    ],
    "puberty": [
        "smri_thick_cdk_paracnrh",
        "smri_thick_cdk_paracnlh",
        "smri_thick_cdk_suplrh",
        "smri_thick_cdk_precnlh",
        "smri_thick_cdk_ptcatelh",
        "smri_thick_cdk_supllh",
        "smri_thick_cdk_sufrlh",
        "smri_thick_cdk_pcrh",
        "smri_thick_cdk_locclh",
        "smri_thick_cdk_iftmlh",
        "smri_thick_cdk_mobfrlh",
        "smri_thick_cdk_loccrh",
        "smri_thick_cdk_mobfrrh",
        "smri_thick_cdk_parsopcrh",
        "smri_thick_cdk_sufrrh",
        "smri_thick_cdk_precnrh",
        "smri_thick_cdk_postcnrh",
        "smri_area_cdk_postcnlh",
        "smri_area_cdk_trvtmlh",
        "smri_vol_cdk_precnlh",
        "smri_vol_cdk_paracnlh",
        "smri_vol_cdk_precnrh",
        "smri_vol_cdk_postcnlh",
        "smri_vol_cdk_postcnrh",
        "smri_vol_cdk_paracnrh",
        "smri_vol_cdk_trvtmlh"
    ],
    "tiv": [
        "smri_thick_cdk_paracnrh",
        "smri_thick_cdk_paracnlh",
        "smri_thick_cdk_suplrh",
        "smri_thick_cdk_precnlh",
        "smri_thick_cdk_sufrlh",
        "smri_thick_cdk_ptcatelh",
        "smri_thick_cdk_supllh",
        "smri_thick_cdk_pcrh",
        "smri_thick_cdk_locclh",
        "smri_thick_cdk_iftmlh",
        "smri_thick_cdk_mobfrlh",
        "smri_thick_cdk_mobfrrh",
        "smri_thick_cdk_sufrrh",
        "smri_area_cdk_postcnlh",
        "smri_vol_cdk_precnlh",
        "smri_vol_cdk_precnrh",
        "smri_vol_cdk_paracnlh",
        "smri_vol_cdk_postcnlh",
        "smri_vol_cdk_linguallh",
        "smri_vol_cdk_postcnrh",
        "smri_vol_cdk_paracnrh"
    ],
    "tiv_puberty": [
        "smri_thick_cdk_paracnrh",
        "smri_thick_cdk_paracnlh",
        "smri_thick_cdk_suplrh",
        "smri_thick_cdk_precnlh",
        "smri_thick_cdk_sufrlh",
        "smri_thick_cdk_ptcatelh",
        "smri_thick_cdk_supllh",
        "smri_thick_cdk_pcrh",
        "smri_thick_cdk_locclh",
        "smri_thick_cdk_iftmlh",
        "smri_thick_cdk_mobfrlh",
        "smri_thick_cdk_mobfrrh",
        "smri_thick_cdk_sufrrh",
        "smri_area_cdk_postcnlh",
        "smri_vol_cdk_precnlh",
        "smri_vol_cdk_precnrh",
        "smri_vol_cdk_paracnlh",
        "smri_vol_cdk_postcnlh",
        "smri_vol_cdk_linguallh",
        "smri_vol_cdk_postcnrh",
        "smri_vol_cdk_paracnrh"
    ]
}
# QCd hormone variables
hormones = [
    "filtered_dhea",
    "filtered_ert",
    "filtered_hse"
]
# add MRI qc variables to brain variable list
all_brain_vars = list(
    np.unique(
        list(chain.from_iterable(significant_brain.values()))
    )
) + ['imgincl_t1w_include', 'mrif_score']
# pull all variables listed above
brain_data = abcdw.data_grabber(
    ABCD_DIR, 
    all_brain_vars, 
    eventname='baseline_year_1_arm_1'
)
# quality control sMRI data using mrif_score and imgincl_t1w_include
qc_ppts = abcdw.smri_qc(brain_data)
# subset brain_data to only include participants with sufficient-quality T1s
brain_data = brain_data.loc[qc_ppts]

# read in processed hormone data, see 0.0hormone-qc.py for details
hormone_data = pd.read_pickle(
    join(PROJ_DIR, DATA_DIR, "qcd_hormones.pkl")
).xs('baseline_year_1_arm_1', level=1, axis=0)[hormones + ['demo_sex_v2']]

# merge the two datasets
big_df = pd.concat([brain_data, hormone_data], axis=1)
# no boys allowed
big_df = big_df[big_df['demo_sex_v2'] == 2.0]

# compute Spearman correlations per brain variable, per hormone
# for each of the aforementioned covariate schemes
stats = ['r', 'p']
for covar in significant_brain.keys():
    brain_vars = significant_brain[covar]
    # separate dataframe for each covariate scheme
    df = pd.DataFrame(
        index=brain_vars,
        columns=pd.MultiIndex.from_product([hormones, stats]),
        dtype=float
    )
    for brain in brain_vars:
        for hormone in hormones:
            temp = pd.concat(
                [
                    brain_data[brain], 
                    hormone_data[hormone]
                ],
                axis=1
            )
            r,p = spearmanr(temp, nan_policy='omit')
            df.at[brain, (hormone, 'r')] = r
            df.at[brain, (hormone, 'p')] = p
    df.to_csv(join(PROJ_DIR, OUTP_DIR, f'brain_x_hormone-corr-{covar}.csv'))