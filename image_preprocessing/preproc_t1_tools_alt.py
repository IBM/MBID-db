import numpy as np
import ants as an
from pathlib import Path
from deepbrain import Extractor
from scipy.stats import pearsonr
import pdb
'''
Functions for fast preprocessing of anatomical images
'''


def extract_brain(image):
    '''
    Brain Extraction

    Parameters
    ----------
    image: ANTs image object

    Returns
    -------
    masked_img: ANTs image object

    mask_img: ANTs image object
    '''
    ext = Extractor()
    prob = ext.run(image.view())
    mask = prob > 0.5

    mask_img = image.new_image_like(mask.astype('float'))
    masked_arr = np.zeros(mask.shape)
    masked_arr[mask] = image.numpy()[mask]
    masked_img = image.new_image_like(masked_arr)
    return masked_img, mask_img


def register_rigid_n_scale(fixed_img, mov_img):
    '''
    Registration to template: rigid with global scaling

    Parameters
    ----------
    fixed_img: path-like or ANTs image object
    mov_img: ANTs image object

    Returns
    -------
    reg_img: ANTs image object
    transforms: (list of strings) path to mat files with transforms
    '''

    if isinstance(fixed_img, Path):
        fixed_img = an.image_read(fixed_img.as_posix())
    elif isinstance(fixed_img, str):
        fixed_img = an.image_read(fixed_img)

    r_out = an.registration(fixed_img, mov_img,
                            type_of_transform='Similarity')
    reg_img = r_out['warpedmovout']
    transforms = r_out['fwdtransforms']
    return reg_img, transforms

def image_template_mask_correlation(registered_img, template_img, mask_img):
    # Load files
    if isinstance(registered_img, Path):
        registered_img = an.image_read(registered_img.as_posix())
    elif isinstance(registered_img, str):
        registered_img = an.image_read(registered_img)
    # Compute Pearson correlation inside mask
    x = registered_img.numpy()[mask_img.numpy().astype(bool)]
    y = template_img.numpy()[mask_img.numpy().astype(bool)]
    r = pearsonr(x, y)[0]
    return r

def run_preproc(in_fn, template_img, mask_img_template):
    '''
    Full preprocessing: bias correction, brain extraction, affine register
    to template

    Parameters
    ----------
    in_fn: path-like
    template_img: ANTs image object, or path-like

    Returns
    -------
    registed_img: ANTs image object
    transform: (string) path to mat file with affine transform
    '''

    # Load template:
    if isinstance(template_img, Path):
        template_img = an.image_read(template_img.as_posix())
    elif isinstance(template_img, str):
        template_img = an.image_read(template_img)
    if isinstance(mask_img_template, Path):
        mask_img_template = an.image_read(mask_img_template.as_posix())
    elif isinstance(mask_img_template, str):
        mask_img_template = an.image_read(mask_img_template)
    # Load movable image:
    mov = an.image_read(in_fn.as_posix())

    # Brain extraction:
    masked_img, mask_img = extract_brain(mov)

    # Bias field correction:
    corr = an.n4_bias_field_correction(masked_img, mask_img)


    # Registration to template:
    registered_img, transforms = register_rigid_n_scale(template_img, corr)

    # Calculate template mask correlation
    correlation = image_template_mask_correlation(registered_img, template_img, mask_img_template)


    return registered_img, transforms, mask_img, correlation

