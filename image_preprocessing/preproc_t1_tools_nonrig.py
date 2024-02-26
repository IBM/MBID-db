import numpy as np
import ants as an
from pathlib import Path
from deepbrain import Extractor
from scipy.stats import pearsonr

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
    masked_img: ANTs image object, extracted brain
    mask_img: ANTs image object, mask to do brain extraction
    '''
    ext = Extractor()
    prob = ext.run(image.view())
    mask = prob > 0.5

    mask_img = image.new_image_like(mask.astype('float'))
    masked_arr = np.zeros(mask.shape)
    masked_arr[mask] = image.numpy()[mask]
    masked_img = image.new_image_like(masked_arr)
    return masked_img, mask_img


def register_rigid_n_scale(fixed_img, mov_img, out_prefix=None):
    '''
    Registration to template: rigid with global scaling

    Parameters
    ----------
    fixed_img: path-like or ANTs image object, template brain
    mov_img: ANTs image object, brain to be mapped to template
    out_prefix: ANTs output prefix to define temp filenames (and directory
                where files would be dumped). Default: None

    Returns
    -------
    reg_img: ANTs image object, resulting brain after registration
    transforms: (list of strings) path to mat files with transforms
    '''

    if isinstance(fixed_img, Path):
        fixed_img = an.image_read(fixed_img.as_posix())
    elif isinstance(fixed_img, str):
        fixed_img = an.image_read(fixed_img)

    if out_prefix is None:
        r_out = an.registration(fixed_img, mov_img,
                                type_of_transform='Similarity')
    else:
        r_out = an.registration(fixed_img, mov_img,
                                type_of_transform='Similarity',
                                outprefix=out_prefix)
    reg_img = r_out['warpedmovout']
    transforms = r_out['fwdtransforms']
    
    return reg_img, transforms


def register_nonrigid(fixed_img, mov_img, init_transf, metric='SyNOnly',
                      out_prefix=None):
    '''
    Registration to template: nonlinear; initial regid+scaling & warping

    Parameters
    ----------
    fixed_img: path-like or ANTs image object, template brain
    mov_img: ANTs image object, brain to be mapped to template
    init_transf: (list of strings) path to mat files with transforms to be
                 applied prior to nonlinear registration
    metric: type of transformation to apply
    out_prefix: ANTs output prefix to define temp filenames (and directory
                where files would be dumped). Default: None

    Returns
    -------
    reg_img: ANTs image object, resulting brain after registration
    transforms: (list of strings) path to mat files with applied transforms
    
    Note: The nonlinear registration now uses the option
    [initial_transform='identity'] to avoid the application of an additional
    transform prior to estimating the warping one. Additional details available
    at https://github.com/ANTsX/ANTsPy/issues/119
    '''

    if isinstance(fixed_img, Path):
        fixed_img = an.image_read(fixed_img.as_posix())
    elif isinstance(fixed_img, str):
        fixed_img = an.image_read(fixed_img)

    r_rig = an.apply_transforms(fixed_img, mov_img, transformlist=init_transf)
    
    if out_prefix is None:
        r_nlin = an.registration(fixed_img, r_rig,
                                 initial_transform='identity',
                                 type_of_transform=metric)
    else:
        r_nlin = an.registration(fixed_img, r_rig,
                                 initial_transform='identity',
                                 type_of_transform=metric,
                                 outprefix=out_prefix)
    
    reg_img = r_nlin['warpedmovout']
    transforms = r_nlin['fwdtransforms']
    
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


def run_preproc(in_fn, template_img, mask_img_template, init_transf=None,
                reg='rigid', out_prefix=None):
    '''
    Full preprocessing: bias correction, brain extraction, affine register
    to template

    Parameters
    ----------
    in_fn: path-like
    template_img: ANTs image object, or path-like
    mask_img_template: ANTs image object, mask of template image
    init_transf: (list of strings) path files with transforms to be
                 applied prior to nonlinear registration (if applicable)
    reg: type of registration to be applied (rigid or nonlinear)
    out_prefix: ANTs output prefix to define temp filenames (and directory
                where files would be dumped). Default: None

    Returns
    -------
    registered_img: ANTs image object
    transforms: (list of strings) path to mat files with applied transforms
    mask_img: mask used to do brain extraction of the input image (brain)
    correlation: within-brain correlation between the registered image and the
                 template (e.g., MNI)
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

    if reg == 'rigid':
        # Rigid Registration to template
        registered_img, transforms = register_rigid_n_scale(template_img,
                                                            corr,
                                                            out_prefix=out_prefix)
    elif reg == 'nonlinear':
        # Nonlinear registration to template
        assert isinstance(init_transf, list), \
            f'Expected a list of <str>, got {type(init_transf)} instead'
        assert all(map(lambda x: isinstance(x, str), init_transf)), \
            ('Expected a list of <str>, got a list of '
             f'{type(init_transf[0])} instead')
        registered_img, transforms = register_nonrigid(template_img, corr,
                                                       init_transf,
                                                       metric='SyNOnly',
                                                       out_prefix=out_prefix)

    # Calculate template mask correlation
    correlation = image_template_mask_correlation(registered_img, template_img,
                                                  mask_img_template)


    return registered_img, transforms, mask_img, correlation
