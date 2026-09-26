import { Component, inject, Input } from '@angular/core';
import { CartService } from '../../../core/services/cart.service';
import { CurrencyPipe } from '@angular/common';
import { ConfirmationToken } from '@stripe/stripe-js/dist/api/confirmation-tokens';
import { AddressPipePipe } from "../../../shared/pipes/address-pipe-pipe";
import { PaymentCardPipe } from "../../../shared/pipes/payment-card-pipe";

@Component({
  selector: 'app-checkout-review',
  imports: [CurrencyPipe,
    AddressPipePipe,
    PaymentCardPipe],
  templateUrl: './checkout-review.component.html',
  styleUrl: './checkout-review.component.scss',
})
export class CheckoutReviewComponent {
  cartService = inject(CartService);
  @Input() confirmationToken?: ConfirmationToken;
}
