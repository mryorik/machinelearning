X = [1, 0.4, 10, 500; 1, 0.7, 7, 480; 1, 1, 5, 740];
y = [18; 24; 54];
%X = [1, 1; 1, 2];
%y = [1; 2];
alpha = 0.000005;

m = size(X, 1);
n = size(X, 2);
Theta = zeros(n, 1);
Theta_new = Theta;

for i = 1:1:n
    s = 0;
    for j = 1:1:m
        s = s + X(j, i) * (X(j, 1:n) * Theta - y(j)) ;
    end
    Theta_new(i) = Theta(i) - alpha * s / m;
end
Theta = Theta_new;
J = sum((X * Theta - y).^2) / (2 * m);

for i = 1:1:n
    s = 0;
    for j = 1:1:m
        s = s + X(j, i) * (X(j, 1:n) * Theta - y(j)) ;
    end
    Theta_new(i) = Theta(i) - alpha * s / m;
end
Theta = Theta_new;
J_new = sum((X * Theta - y).^2) / (2 * m);

while abs(J - J_new) > 0.0001
    J - J_new
    J = J_new;
    for i = 1:1:n
        s = 0;
        for j = 1:1:m
            s = s + X(j, i) * (X(j, 1:n) * Theta - y(j)) ;
        end
        Theta_new(i) = Theta(i) - alpha * s / m;
    end
    Theta = Theta_new;
    J_new = sum((X * Theta - y).^2) / (2 * m);
end
h = X * Theta;
