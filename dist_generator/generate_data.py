import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def generate_normal_data_points(mean, std_dev, num_points, num_dimensions, seed):
    """
    Generate normally distributed data points.

    Parameters:
        mean (float): Mean of the normal distribution.
        std_dev (float): Standard deviation of the normal distribution.
        num_points (int): Number of data points to generate.
        num_dimensions (int): Number of dimensions for each data point.
        seed (int): Random seed for reproducibility.

    Returns:
        np.ndarray: Generated data points.
    """
    np.random.seed(seed)
    return np.random.normal(mean, std_dev, size=(num_points, num_dimensions))

def generate_uniform_data_points(min_val, max_val, num_points, num_dimensions, seed):
    """
    Generate uniformly distributed data points.

    Parameters:
        min_val (float): Minimum value for the uniform distribution.
        max_val (float): Maximum value for the uniform distribution.
        num_points (int): Number of data points to generate.
        num_dimensions (int): Number of dimensions for each data point.
        seed (int): Random seed for reproducibility.

    Returns:
        np.ndarray: Generated data points.
    """
    np.random.seed(seed)
    return np.random.uniform(min_val, max_val, size=(num_points, num_dimensions))

def generate_correlated_data_points(rho, num_points, num_dimensions, seed):
    """
    Generate correlated data points.

    Parameters:
        rho (float): Correlation coefficient.
        num_points (int): Number of data points to generate.
        num_dimensions (int): Number of dimensions for each data point.
        seed (int): Random seed for reproducibility.

    Returns:
        np.ndarray: Generated data points.
    """
    np.random.seed(seed)
    mean = np.zeros(num_dimensions)
    cov_matrix = np.eye(num_dimensions) + rho * (np.ones((num_dimensions, num_dimensions)) - np.eye(num_dimensions))
    return np.random.multivariate_normal(mean, cov_matrix, size=num_points)

def generate_anticorrelated_data_points(rho, num_points, num_dimensions, seed):
    """
    Generate anticorrelated data points.
    This function ensures the covariance matrix for anticorrelation is positive semi-definite before trying to draw samples from it. 
    The added assertion ensures `rho` is in an acceptable range before constructing the covariance matrix. 
    Also, the value check with `np.linalg.eigvalsh` ensures that the matrix is indeed positive semi-definite.

    Please note that when dealing with anticorrelation, the permissible values of `rho` are more constrained compared to the correlation case, 
    since the off-diagonal values can't be too negative without violating the positive semi-definiteness condition.

    Parameters:
        rho (float): Correlation coefficient (positive value expected for anticorrelation).
        num_points (int): Number of data points to generate.
        num_dimensions (int): Number of dimensions for each data point.
        seed (int): Random seed for reproducibility.

    Returns:
        np.ndarray: Generated data points.
    """
    np.random.seed(seed)

    assert 0 <= rho < 1/(num_dimensions-1), "rho must be in [0, 1/(num_dimensions-1)) for positive semi-definiteness."

    mean = np.zeros(num_dimensions)
    anticorr_matrix = np.eye(num_dimensions) - rho * (np.ones((num_dimensions, num_dimensions)) - np.eye(num_dimensions))
    print(anticorr_matrix)
  
    # Ensuring the matrix is positive semi-definite
    eigenvalues = np.linalg.eigvalsh(anticorr_matrix)
    if np.any(eigenvalues <= 0):
        raise ValueError("The generated covariance matrix is not positive semi-definite.")

    return np.random.multivariate_normal(mean, anticorr_matrix, size=num_points)

def write_to_csv(data_points, output_file):
    """
    Write data points to a CSV file.

    Parameters:
        data_points (np.ndarray): Data points to be written.
        output_file (str): Output CSV file name.
    """
    df = pd.DataFrame(data_points, columns=[f'Dim_{i}' for i in range(data_points.shape[1])])
    df.to_csv(output_file, index=False)
    print(f"Data points written to {output_file}")

def plot_scatter(data_points, image_file):
    """
    Plot a scatter plot of data points and save the image.

    Parameters:
        data_points (np.ndarray): Data points to be plotted.
        image_file (str): Output image file name.
    """
    # Only plot the first two dimensions for visualization
    plt.figure(figsize=(8, 8))
    plt.scatter(data_points[:, 0], data_points[:, 1], marker='o', color='blue', alpha=0.7)
    plt.title('Scatter Plot of Data Distribution (First Two Dimensions)')
    plt.xlabel('Dimension 0')
    plt.ylabel('Dimension 1')
    plt.grid(True)
    plt.savefig(image_file)
    print(f"Scatter plot image saved to {image_file}")

def main():
    """
    Main function to parse command line arguments, generate data points, and save to CSV file and plot scatter plot.
    """
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description="Generate random data points and write to a CSV file.")
    
    # Add command line arguments
    parser.add_argument("--generator", choices=['normal', 'uniform', 'correlated', 'anticorrelated'], default='normal', help="Data generator type.")
    parser.add_argument("--mean", type=float, default=0.0, help="Mean (for normal distribution).")
    parser.add_argument("--std_dev", type=float, default=1.0, help="Standard deviation (for normal distribution).")
    parser.add_argument("--min_val", type=float, default=0.0, help="Minimum value (for uniform distribution).")
    parser.add_argument("--max_val", type=float, default=1.0, help="Maximum value (for uniform distribution).")
    parser.add_argument("--rho", type=float, default=0.5, help="Correlation coefficient (for correlated and anticorrelated distribution).")
    parser.add_argument("--num_points", type=int, default=100, help="Number of data points to generate.")
    parser.add_argument("--output_file", default="output.csv", help="Output CSV file name.")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility.")
    parser.add_argument("--num_dimensions", type=int, default=2, help="Number of dimensions for data points.")
    
    # Parse command line arguments
    args = parser.parse_args()

    # Generate data points based on user input
    if args.seed is not None:
        np.random.seed(args.seed)

    # Generate data points based on user input
    if args.generator == 'normal':
        data_points = generate_normal_data_points(args.mean, args.std_dev, args.num_points, args.num_dimensions, args.seed)
    elif args.generator == 'uniform':
        data_points = generate_uniform_data_points(args.min_val, args.max_val, args.num_points, args.num_dimensions, args.seed)
    elif args.generator == 'correlated':
        data_points = generate_correlated_data_points(args.rho, args.num_points, args.num_dimensions, args.seed)
    elif args.generator == 'anticorrelated':
        data_points = generate_anticorrelated_data_points(args.rho, args.num_points, args.num_dimensions, args.seed)
    else:
        raise ValueError("Invalid generator type. Supported types: normal, uniform, correlated, anticorrelated")

    # Write data points to CSV file
    write_to_csv(data_points, args.output_file)

    # Plot scatter plot and save image
    plot_scatter(data_points, f'{args.output_file}_image.png')

if __name__ == "__main__":
    main()
