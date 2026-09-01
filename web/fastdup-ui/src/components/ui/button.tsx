import type { ButtonHTMLAttributes } from "react";

type Props = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "danger" | "ghost";
};

export function Button({ variant = "secondary", className = "", ...props }: Props) {
  return <button className={`button button-${variant} ${className}`} {...props} />;
}
