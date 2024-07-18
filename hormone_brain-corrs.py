import numpy as np
import pandas as pd
import abcdWrangler as abcdw

from os.path import join
from itertools import chain
from scipy.stats import spearmanr

ABCD_DIR = "/Volumes/projects_herting/LABDOCS/PROJECTS/ABCD/Data/release5.1/abcd-data-release-5.1"
PROJ_DIR = "/Volumes/projects_herting/LABDOCS/Personnel/Katie/kangaroo_aim1"
DATA_DIR = "data"
OUTP_DIR = "output"
FIGS_DIR = "figures"

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

hormones = [
    "filtered_dhea",
    "filtered_ert",
    "filtered_hse"
]

all_brain_vars = list(
    np.unique(
        list(chain.from_iterable(significant_brain.values()))
    )
)

brain_data = abcdw.data_grabber(
    ABCD_DIR, 
    all_brain_vars, 
    eventname='baseline_year_1_arm_1'
)

hormone_data = pd.read_pickle(
    join(PROJ_DIR, DATA_DIR, "qcd_hormones.pkl")
).xs('baseline_year_1_arm_1', level=1, axis=0)[hormones]

stats = ['r', 'p']
for covar in significant_brain.keys():
    brain_vars = significant_brain[covar]
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
    df.to_csv(join(PROJ_DIR, OUTP_DIR, f'brain_x_hormone-corr_{covar}.csv'))