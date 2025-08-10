import React from "react";

const LoadingSpinner = ({
  size = 6,
  text = "Loading...",
  textPosition = "right",
}) => {
  const spinnerSize = `h-${size} w-${size}`;
  const textClass = textPosition === "bottom" ? "mt-2" : "ml-2";

  return (
    <div
      className={`flex items-center ${textPosition === "bottom" ? "flex-col" : ""}`}
    >
      <div
        className={`animate-spin rounded-full ${spinnerSize} border-b-2 border-white`}
      ></div>
      {text && <span className={textClass}>{text}</span>}
    </div>
  );
};

export default LoadingSpinner;
